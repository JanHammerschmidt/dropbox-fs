import logging
import sys
import os
import signal
from threading import Thread
from pathlib import Path
from fuse import FUSE
from dropbox_fs.crawler import DropboxCrawler
from dropbox_fs.fs import DropboxFs
from dropbox_fs.misc import wait_for_event

log = logging.getLogger(__name__)


def exit_handler(signum, frame):
    global stop_request
    log.info("Waiting for crawler thread to finish (this might take around 30s)")
    stop_request = True
    signal.signal(signal.SIGINT, original_sigint)
    try:
        if not wait_for_event(crawler._finished, 60):
            if os.name == 'nt':
                log.error('Thread timed out! You might have to kill this process..')
            else:
                log.error('Thread timed out! Data may be lost')
                sys.exit(1)
    except KeyboardInterrupt:
        if os.name == 'nt':
            log.error('The worker thread is not responding. You might have to kill the process manually..')
        else:
            log.warning('Exiting anyway.. (data may be lost!)')
        sys.exit(1)
    sys.exit(0)


def dropbox_fs():
    log.info('starting file system on z:')
    FUSE(fs, "z:", foreground=True, ro=True)


def start_fs():
    Thread(target=dropbox_fs).start()


def main():
    import argparse

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('action', type=str, choices=['init', 'load'], nargs='?', default='load')
    parser.add_argument('-t', '--token', type=str)
    parser.add_argument('-p', '--path', type=str, default='')
    parser.add_argument('-l', '--local-folder', type=str, default=None)
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s [%(name)-18.18s] [%(levelname)-5.5s] %(message)s"
        # level=log_level
    )
    logging.getLogger('dropbox_fs').setLevel(log_level)

    global crawler, original_sigint, fs
    crawler = DropboxCrawler(start_fs)
    if args.action == 'init':
        if args.token is None:
            args.error('initialization requires a dropbox token')
        if args.local_folder is None:
            log.warning('No local dropbox folder specified')
            local_folder = None
        else:
            local_folder = Path(args.local_folder)
            if not local_folder.exists():
                args.error('Local dropbox folder not found')
        crawler.init(args.token, args.path, local_folder)
    elif args.action == 'load':
        if not crawler.load_snapshot():
            return

    fs = DropboxFs(crawler)
    Thread(target=crawler.crawl).start()
    original_sigint = signal.signal(signal.SIGINT, exit_handler)
    if os.name != 'nt':
        signal.pause()
    else:
        try:
            from time import sleep
            while True:
                sleep(10)
        except KeyboardInterrupt:
            pass
