import os
import logging
import stat
import errno
from time import time
from fuse import FuseOSError, Operations, LoggingMixIn

from dropbox_fs.crawler import DropboxCrawler, File
from dropbox_fs.cache import FileCache

log = logging.getLogger(__name__)


class DropboxFs(LoggingMixIn, Operations):
    def __init__(self, crawler: DropboxCrawler, file_cache: FileCache):
        # super().__init__()
        self.root = crawler.root
        self.file_cache = file_cache
        self.db_base_path = crawler._db_base_path
        if len(self.db_base_path) <= 0 or self.db_base_path[-1] != '/':
            self.db_base_path += '/'

        self.local_folder = crawler._local_folder / self.db_base_path[1:]
        self.time_created = time()
        self.folder_attr = dict(st_mode=(stat.S_IFDIR | 0o777), st_nlink=1)
        for t in ['st_ctime', 'st_mtime', 'st_atime']:
            self.folder_attr[t] = self.time_created
        self.file_attr_base = dict(st_mode=(stat.S_IFREG | 0o666), st_nlink=1, st_ctime=self.time_created)

    def readdir(self, path, fh):
        log.debug('readdir {} {}'.format(path, fh))
        folder = self.find_folder(path)  # os.path.normpath
        if folder is None:
            log.warning('unknown path: {}'.format(path))
            return ['.', '..']
        return ['.', '..'] + list(folder.folders.keys()) + list(folder.files.keys())

    def file_attr(self, file: File):
        attr = self.file_attr_base.copy()
        attr['st_size'] = file.size
        modified = file.modified.timestamp()
        attr['st_mtime'] = modified
        attr['st_atime'] = modified
        return attr

    def getattr(self, path, fh=None):
        # log.debug('getattr {} {}'.format(path, fh))
        local = self.local_folder / path[1:]
        if local.exists():
            st = os.lstat(local)
            return {key: getattr(st, key) for key in
                    ['st_atime', 'st_ctime', 'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid']}
        if path == '/':
            return self.folder_attr
        else:
            folder, item = os.path.split(path)
            folder = self.find_folder(folder)
            if folder is None:
                raise FuseOSError(errno.ENOENT)
            elif item in folder.folders:
                return self.folder_attr
            elif item in folder.files:
                return self.file_attr(folder.files[item])
            else:
                raise FuseOSError(errno.ENOENT)

    def find_folder(self, path):
        cur_folder = self.root
        if path != '/':
            hierarchy = path[1:].split('/')  # os.path.sep <= path must be normpath'ed for that..
            for folder in hierarchy:
                if folder in cur_folder.folders:
                    cur_folder = cur_folder.folders[folder]
                else:
                    return None
        return cur_folder

    def open(self, path, flags):
        rel_path = path[1:]
        local = self.local_folder / rel_path
        if local.exists():
            log.debug('open locally: {}'.format(path))
            return self.file_cache.open_file(local, flags)
        folder, item = os.path.split(path)
        folder = self.find_folder(folder)
        if item in folder.files:
            log.debug('trying to open from cache: {}'.format(path))
            return self.file_cache.open(path, rel_path, folder.files[item], self.db_base_path + rel_path, flags)
        else:
            return 0

    def read(self, path, size, offset, fh):
        if fh == 0:
            raise FuseOSError(errno.EIO)
        log.debug('read {} @ {}: {}'.format(size, offset, path))
        return self.file_cache.read(path, size, offset, fh)

    def release(self, path, fh):
        log.debug('close {}'.format(path))
        self.file_cache.close(fh)

    # access = None
    # flush = None
    # getxattr = None
    # listxattr = None
    # open = None
    # opendir = None
    # release = None
    # releasedir = None
    # statfs = None
