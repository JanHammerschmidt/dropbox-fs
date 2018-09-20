import contextlib
import logging
import errno
from typing import List
from enum import Enum
from pathlib import Path
from threading import Thread, Event
from fuse import FuseOSError
from requests.exceptions import ReadTimeout
from dropbox import Dropbox
from dropbox_fs.crawler import File

log = logging.getLogger(__name__)


class SizeWatcher(Event):
    def __init__(self, size):
        super().__init__()
        self.size = size


class FileDownloader:
    class State(Enum):
        working = 0
        success = 1
        failure = 2

    def __init__(self, path: str, file: Path, dbx: Dropbox, db_path: str, finished_callback):
        self.path, self.file, self.dbx, self.db_path = path, file, dbx, db_path
        self.state = self.State.working
        self.finished_callback = finished_callback
        self.size_watcher: List[SizeWatcher] = []

        file.parent.mkdir(parents=True, exist_ok=True)
        self.f = open(str(file), 'wb')
        self.bytes_downloaded = 0
        Thread(target=self.download).start()

    def download(self):
        log.debug('downloading {}'.format(self.db_path))
        with contextlib.closing(self):
            with contextlib.closing(self.f):
                try:
                    md, res = self.dbx.files_download(self.db_path)
                    with contextlib.closing(res):
                        for c in res.iter_content(2 ** 16):
                            self.f.write(c)
                            self.bytes_downloaded += len(c)
                            [w.set() for w in self.size_watcher if self.bytes_downloaded > w.size]
                except (ConnectionError, ReadTimeout):
                    log.error('downloading failed for {}'.format(self.db_path))
                    self.state = self.State.failure
                    [w.set() for w in self.size_watcher]
                    return

            self.state = self.State.success
            log.debug('download finished: {}'.format(self.db_path))
            [w.set() for w in self.size_watcher]

    def close(self):
        self.finished_callback(self)

    def wait_for_size(self, size) -> bool:
        log.debug('waiting for size {}: {}'.format(size, self.db_path))
        if self.state == self.State.working and self.bytes_downloaded < size:
            log.debug('new size watcher for {}'.format(self.db_path))
            watcher = SizeWatcher(size)
            self.size_watcher.append(watcher)
            while self.bytes_downloaded < size and self.state == self.State.working:
                watcher.wait(2)
            self.size_watcher.remove(watcher)
        if self.state == self.State.failure:
            return False
        if not self.f.closed:
            log.debug('flush {}'.format(self.db_path))
            self.f.flush()
        return True


class FileCache:
    def __init__(self, base_path: Path, dbx: Dropbox):
        self.base_path = base_path
        self.dbx = dbx
        self.downloading = {}
        self.files_opened = {}

    def open(self, path: str, rel_path: str, db_file: File, db_path: str, flags: int) -> int:
        file = self.base_path / rel_path

        if file.exists():
            if db_path in self.downloading:
                return self.open_file(file, flags)
            return self.open_file(file, flags)
            # downloading ..?
            # newer ..?
        else:
            self.downloading[path] = FileDownloader(path, file, self.dbx, db_path, self.finished_downloading)
            return self.open_file(file, flags)

    def read(self, path, size, offset, fh):
        try:
            f = self.files_opened[fh]
        except KeyError:
            log.error('no open file found while reading from {}'.format(path))
            raise FuseOSError(errno.EIO)
        if path in self.downloading:
            if not self.downloading[path].wait_for_size(offset+size):
                raise FuseOSError(errno.EIO)
        # else:
        #     log.debug('{} not in {}'.format(path, list(self.downloading.keys())))
        f.seek(offset)
        return f.read(size)

    def close(self, fh):
        try:
            self.files_opened.pop(fh).close()
        except KeyError:
            log.error('no open file found while closing file handle {}'.format(fh))

    def finished_downloading(self, downloader: FileDownloader):
        log.debug('removing {} from downloading'.format(downloader.db_path))
        del self.downloading[downloader.path]

    def open_file(self, file, _flags) -> int:
        f = open(file, 'rb')
        self.files_opened[f.fileno()] = f
        return f.fileno()