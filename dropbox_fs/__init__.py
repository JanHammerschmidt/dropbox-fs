import sys, os, shutil, signal, logging, time
import dropbox, pickle
from datetime import datetime
from threading import Thread, Event
from dropbox.exceptions import ApiError, AuthError
from dropbox.files import FileMetadata, FolderMetadata

# https://www.dropbox.com/developers/documentation/http/documentation#files-list_folder-continue

db_token = ''  # insert own db token here
db_base_path = ''  # for dropbox root use ''. Otherwise prepend a '/'


log = logging.getLogger(__name__)

finished = Event()
stop_request = False
finished_crawling = False
data_file = 'data.pkl'
last_save = datetime.now()
save_interval = 120  # periodically save every n seconds
save_interval_entries = 500  # save when n items have been updated
updated_entries = 0  # count how many entries have been updated
data_version = 1  # bump this on changes how the data is saved

def init_dropbox():
    global dbx
    log.info('connecting to dropbox')
    dbx = dropbox.Dropbox(db_token)

    # Make sure that the access token is valid
    # try:
    #     dbx.users_get_current_account()
    # except AuthError as err:
    #     sys.exit("ERROR: Invalid access token; try re-generating an access token from the app console on the web.")


def remove_from_dict_case_insensitive(dict, key):
    key_lower = key.lower()
    existing_key = next((k for k in dict.keys() if k.lower() == key_lower), None)
    if existing_key is not None:
        log.debug('change {} to {}'.format(existing_key, key))
        del dict[existing_key]


def update_tree(data):
    global updated_entries
    log.debug('new data (%i entries)' % len(data.entries))
    updated_entries += len(data.entries)
    for e in data.entries:
        path_components = e.path_display[1:].split('/')
        folder = root
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


def crawl():
    global space_used, space_allocated

    log.info('get space usage..')
    data = dbx.users_get_space_usage()
    space_used = data.used
    space_allocated = data.allocation.get_individual().allocated

    global crawl_cursor, finished_crawling
    if not finished_crawling:
        log.info('start crawling..')
        if crawl_cursor is None:
            data = dbx.files_list_folder(db_base_path, recursive=True)
            crawl_cursor = update_tree(data)
        while not stop_request:
            data = dbx.files_list_folder_continue(crawl_cursor)
            crawl_cursor = update_tree(data)
            if not data.has_more:
                log.info('no further data')
                finished_crawling = True
                save_data()
                break

    log.info('poll for changes..')
    global update_cursor
    while not stop_request:
        log.debug('longpoll')
        changes = dbx.files_list_folder_longpoll(update_cursor, timeout=30)
        # TODO: what if `changes.backoff is not None`?
        if changes.changes:
            data = dbx.files_list_folder_continue(update_cursor)
            update_cursor = update_tree(data)
        if stop_request:
            break
        if (datetime.now() - last_save).total_seconds() > save_interval or updated_entries >= save_interval_entries:
            save_data()

    save_data()
    finished.set()
    log.info('Worker thread exited normally')


class File:
    def __init__(self, name, metadata):
        self.name = name
        self.size = metadata.size
        self.modified = metadata.client_modified


class Folder:
    def __init__(self, name, files=None, folders=None):
        self.name = name
        self.files = {} if files is None else {f.name: f for f in files}
        self.folders = {} if folders is None else {f.name: f for f in folders}


def load_data():
    global root, crawl_cursor, update_cursor, finished_crawling, space_used, space_allocated, last_save, db_base_path
    try:
        with open(data_file, 'rb') as f:
            data = pickle.load(f)
        if data['data_version'] != data_version:
            log.error('incompatible versions of script ({}) and data file ({})'.format(data_version, data['data_version']))
            raise RuntimeError('loading failed')
        db_base_path = data['root_path']
        root = data['root']
        crawl_cursor = data['crawl_cursor']
        update_cursor = data['update_cursor']
        finished_crawling = data['finished_crawling']
        space_used = data['space_used']
        space_allocated = data['space_allocated']
        last_save = datetime.fromtimestamp(data['last_save'])
        log.info('successfully loaded data')
        return True
    except:
        log.info("loading data failed")
        root = Folder('root')
        crawl_cursor, finished_crawling = None, False
        log.debug("getting update cursor")
        update_cursor = dbx.files_list_folder_get_latest_cursor(db_base_path, recursive=True, include_deleted=True).cursor
    return False


def save_data():
    global last_save, updated_entries
    log.debug('save data to %s' % data_file)
    was_finished = finished.is_set()
    finished.clear()  # don't kill the process during saving data!
    try:
        shutil.move(data_file, 'data.prev.pkl')
    except:
        pass
    last_save = datetime.now()
    data = {
        'data_version': data_version,
        'root_path': db_base_path,
        'root': root,
        'crawl_cursor': crawl_cursor,
        'update_cursor': update_cursor,
        'finished_crawling': finished_crawling,
        'space_used': space_used,
        'space_allocated': space_allocated,
        'last_save': last_save.timestamp()
    }
    with open(data_file, 'wb') as f:
        pickle.dump(data, f)
    updated_entries = 0
    if was_finished:
        finished.set()


