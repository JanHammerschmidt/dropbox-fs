import logging
import sys
import os
import signal
from threading import Thread

from dropbox_fs.crawler import DropboxCrawler
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
    from dropbox_fs.fs import DropboxFs
    from fuse import FUSE
    global fuse
    fuse = FUSE(DropboxFs(crawler.root), "z:", foreground=False, ro=True)


def start_fs():
    Thread(target=dropbox_fs).start()


def main():
    import argparse

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('action', type=str, choices=['init', 'load'], default='load')
    parser.add_argument('-t', '--dropbox-token', type=str)
    parser.add_argument('-p', '--dropbox-path', type=str, default='')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s [%(name)-18.18s] [%(levelname)-5.5s] %(message)s",
        level=log_level
    )
    log.setLevel(log_level)

    global crawler, original_sigint
    crawler = DropboxCrawler(start_fs)
    if args.action == 'init':
        if args.dropbox_token is None:
            args.error('initialization requires a dropbox token')
        crawler.init(args.dropbox_token, args.dropbox_path)
    elif args.action == 'load':
        crawler.load_snapshot()

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
