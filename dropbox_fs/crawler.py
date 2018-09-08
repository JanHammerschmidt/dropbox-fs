import logging
import shutil
import pickle
import dropbox
from datetime import datetime
from pathlib import Path
from threading import Event
from requests.exceptions import ReadTimeout, ConnectionError
from dropbox.exceptions import ApiError, AuthError
from dropbox.files import FileMetadata, FolderMetadata
from .misc import remove_from_dict_case_insensitive

data_version = 4  # bump this on changes how the data is saved
data_file = 'data.pkl'

# https://www.dropbox.com/developers/documentation/http/documentation#files-list_folder-continue

log = logging.getLogger(__name__)


class File:
    def __init__(self, name, metadata: FileMetadata):
        self.name = name
        self.size = metadata.size
        self.modified = metadata.server_modified


class Folder:
    def __init__(self, name, files=None, folders=None):
        self.name = name
        self.files = {} if files is None else {f.name: f for f in files}
        self.folders = {} if folders is None else {f.name: f for f in folders}


class DropboxCrawler:
    root: Folder
    dbx: dropbox.Dropbox
    _update_cursor: str
    _crawl_cursor: str
    _finished_crawling: bool
    _db_token: str
    _db_base_path: str
    _local_folder: Path

    def __init__(self, finished_initial_crawl_callback=lambda: None):
        """ You must either call `init` or `load_snapshot` to get things going."""

        self.save_interval = 120  # periodically save every n seconds
        self.save_interval_entries = 500  # save when n items have been updated
        self.finished_initial_crawl_callback = finished_initial_crawl_callback

        self.space_used = 0
        self.space_allocated = 0

        self._finished = Event()
        self._stop_request = False
        self._updated_entries = 0  # count how many entries have been updated
        self._last_save = datetime.now()

        self.dbx = None

    def init(self, db_token, db_base_path='', local_folder: Path = None):
        """ db_base_path: for dropbox root use ''. Otherwise prepend a '/' """
        self._db_token = db_token
        self._db_base_path = db_base_path
        self._local_folder = local_folder
        self._crawl_cursor = None
        self._finished_crawling = False
        self.connect()
        self._update_cursor = self.dbx.files_list_folder_get_latest_cursor(self._db_base_path, recursive=True,
                                                                           include_deleted=True).cursor
        self.root = Folder(self._db_base_path)

    def connect(self):
        log.info('Connecting to Dropbox...')
        self.dbx = dropbox.Dropbox(self._db_token)

    def access_token_is_valid(self):
        """ check if the access token is valid """
        try:
            self.dbx.users_get_current_account()
            return True
        except AuthError as err:
            log.error(
                "Invalid access token ({}). "
                "Try re-generating an access token from the app console on the web.".format(str(err))
            )
            return False

    def update_tree(self, data):
        log.debug('new data (%i entries)' % len(data.entries))
        self._updated_entries += len(data.entries)
        for e in data.entries:
            path_components = e.path_display[1:].split('/')
            folder = self.root
            for f in path_components[:-1]:
                try:
                    f_lower = f.lower()
                    folder = next(val for key, val in folder.folders.items() if key.lower() == f_lower)
                except StopIteration:
                    new_folder = Folder(f)
                    folder.folders[f] = new_folder
                    folder = new_folder
            f = path_components[-1]
            if isinstance(e, FileMetadata):
                # log.debug('add/change file {}'.format(e.path_display))
                if f not in folder.files:
                    remove_from_dict_case_insensitive(folder.files, f)
                folder.files[f] = File(f, e)
            elif isinstance(e, FolderMetadata):
                # log.debug('add/change folder {}'.format(e.path_display))
                if f not in folder.folders:
                    remove_from_dict_case_insensitive(folder.folders, f)
                folder.folders[f] = Folder(f)
            else:  # DeletedMetadata
                # log.debug('removing file/folder {}'.format(e.path_display))
                remove_from_dict_case_insensitive(folder.files, f)
                remove_from_dict_case_insensitive(folder.folders, f)
        return data.cursor

    def crawl(self):
        dbx = self.dbx

        log.debug('get space usage..')
        data = dbx.users_get_space_usage()
        self.space_used = data.used
        self.space_allocated = data.allocation.get_individual().allocated

        if not self._finished_crawling:
            log.info('doing initial crawl..')
            if self._crawl_cursor is None:
                data = dbx.files_list_folder(self._db_base_path, recursive=True)
                self._crawl_cursor = self.update_tree(data)
            while not self._stop_request:
                data = dbx.files_list_folder_continue(self._crawl_cursor)
                self._crawl_cursor = self.update_tree(data)
                if not data.has_more:
                    log.info('no further data')
                    self._finished_crawling = True
                    self.save_snapshot()
                    break

        self.finished_initial_crawl_callback()
        log.info('poll for changes..')
        while not self._stop_request:
            log.debug('longpoll')
            try:
                changes = dbx.files_list_folder_longpoll(self._update_cursor, timeout=30)
            except ReadTimeout as e:
                log.warning(e)
                continue
            else:
                # TODO: what if `changes.backoff is not None`?
                if changes.changes:
                    try:
                        data = dbx.files_list_folder_continue(self._update_cursor)
                    except ConnectionError as e:
                        log.warning(e)
                    else:
                        self._update_cursor = self.update_tree(data)
            if self._stop_request:
                break
            if (datetime.now() - self._last_save).total_seconds() > self.save_interval \
                    or self._updated_entries >= self.save_interval_entries:
                self.save_snapshot()

        self.save_snapshot()
        self._finished.set()
        log.info('Worker thread exited normally')

    def load_snapshot(self):
        try:
            with open(data_file, 'rb') as f:
                data = pickle.load(f)
            if data['data_version'] != data_version:
                raise RuntimeError(
                    'incompatible versions of script ({}) and data file ({})'.format(
                        data_version, data['data_version'])
                )
            self._db_base_path = data['root_path']
            self.root = data['root']
            self._local_folder = data['local_folder']
            self._db_token = data['db_token']
            self._crawl_cursor = data['crawl_cursor']
            self._update_cursor = data['update_cursor']
            self._finished_crawling = data['finished_crawling']
            self._last_save = datetime.fromtimestamp(data['last_save'])
            log.info('successfully loaded data')
            self.connect()
            return True
        except RuntimeError as e:
            log.error("loading data failed: {}".format(str(e)))
            return False

    def save_snapshot(self):
        log.debug('save data to %s' % data_file)
        was_finished = self._finished.is_set()
        self._finished.clear()  # don't kill the process during saving data!
        try:
            shutil.move(data_file, 'data.prev.pkl')
        except FileNotFoundError:
            pass
        except shutil.Error as e:
            log.warning("moving {} to {} failed ({})".format(data_file, 'data.prev.pkl', str(e)))
        self._last_save = datetime.now()
        data = {
            'data_version': data_version,
            'root_path': self._db_base_path,
            'root': self.root,
            'local_folder': self._local_folder,
            'db_token': self._db_token,
            'crawl_cursor': self._crawl_cursor,
            'update_cursor': self._update_cursor,
            'finished_crawling': self._finished_crawling,
            'last_save': self._last_save.timestamp()
        }
        with open(data_file, 'wb') as f:
            pickle.dump(data, f)
        self._updated_entries = 0
        if was_finished:
            self._finished.set()
