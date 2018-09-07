import logging
import os
import stat
import errno
from fuse import FuseOSError, Operations, LoggingMixIn

log = logging.getLogger(__name__)


class DropboxFs(LoggingMixIn, Operations):
    def __init__(self, root):
        # super().__init__()
        self.root = root
        self.folder_attr = dict(st_mode=(stat.S_IFDIR | 0o777), st_nlink=1)
        self.file_attr_base = dict(st_mode=(stat.S_IFREG | 0o666), st_nlink=1)

    def readdir(self, path, fh):
        log.debug('readdir {} {}'.format(path, fh))
        folder = self.find_folder(path)  # os.path.normpath
        if folder is None:
            log.warning('unknown path: {}'.format(path))
            return ['.', '..']
        return ['.', '..'] + list(folder.folders.keys()) + list(folder.files.keys())

    def file_attr(self, file):
        attr = self.file_attr_base.copy()
        attr['st_size'] = file.size
        return attr

    def getattr(self, path, fh=None):
        log.debug('getattr {} {}'.format(path, fh))
        if path == '/':
            return self.folder_attr
        else:
            folder, item = os.path.split(path)
            folder = self.find_folder(folder)
            if item in folder.folders:
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

    # access = None
    # flush = None
    # getxattr = None
    # listxattr = None
    # open = None
    # opendir = None
    # release = None
    # releasedir = None
    # statfs = None
