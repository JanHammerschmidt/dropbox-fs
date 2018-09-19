import contextlib
import logging
import errno
from typing import List
from pathlib import Path
from threading import Thread, Event
from fuse import FuseOSError
from dropbox import Dropbox
from dropbox_fs.crawler import File

log = logging.getLogger(__name__)


class SizeWatcher(Event):
    def __init__(self, size):
        super().__init__()
        self.size = size


class FileDownloader:
    def __init__(self, path: str, file: Path, dbx: Dropbox, db_path: str, finished_callback):
        self.path, self.file, self.dbx, self.db_path = path, file, dbx, db_path
        self.successfully_finished = False
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
                md, res = self.dbx.files_download(self.db_path)
                with contextlib.closing(res):
                    for c in res.iter_content(2 ** 16):
                        self.f.write(c)
                        self.bytes_downloaded += len(c)
                        [w.set() for w in self.size_watcher if self.bytes_downloaded > w.size]

            self.successfully_finished = True
            log.debug('download finished: {}'.format(self.db_path))
            [w.set() for w in self.size_watcher]

    def close(self):
        self.finished_callback(self)

    def wait_for_size(self, size):
        log.debug('waiting for size {}: {}'.format(size, self.db_path))
        if not self.successfully_finished and self.bytes_downloaded < size:
            log.debug('new size watcher for {}'.format(self.db_path))
            watcher = SizeWatcher(size)
            self.size_watcher.append(watcher)
            while self.bytes_downloaded < size and not self.successfully_finished:
                watcher.wait(2)
            self.size_watcher.remove(watcher)
        log.debug('flush {}'.format(self.db_path))
        if not self.f.closed:
            self.f.flush()


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
                return self._open_file(file, flags)
            return self._open_file(file, flags)
            # downloading ..?
            # newer ..?
        else:
            self.downloading[path] = FileDownloader(path, file, self.dbx, db_path, self.finished_downloading)
            return self._open_file(file, flags)

    def read(self, path, size, offset, fh):
        try:
            f = self.files_opened[fh]
        except KeyError:
            raise FuseOSError(errno.EIO)
        if path in self.downloading:
            self.downloading[path].wait_for_size(offset+size)
        else:
            log.debug('{} not in {}'.format(path, list(self.downloading.keys())))
        f.seek(offset)
        return f.read(size)

    def close(self, fh):
        try:
            self.files_opened.pop(fh).close()
        except KeyError:
            log.error('no open file found for file handle {}'.format(fh))

    def finished_downloading(self, downloader: FileDownloader):
        log.debug('removing {} from downloading'.format(downloader.db_path))
        assert downloader.successfully_finished
        del self.downloading[downloader.path]

    def _open_file(self, file, _flags) -> int:
        f = open(file, 'rb')
        self.files_opened[f.fileno()] = f
        return f.fileno()