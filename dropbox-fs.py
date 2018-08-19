import os, signal, logging, sys, time, dropbox
from threading import Thread
from dropbox_fs import load_data, crawl, finished, init_dropbox


log = logging.getLogger('dropbox_fs')


def wait_for_event(event, seconds):
    if os.name != 'nt':
        return event.wait(seconds)
    t0 = time.time()
    while (time.time() - t0) < seconds:
        if event.is_set():
            return True
    return False


def exit_handler(signum, frame):
    global stop_request
    log.info("Waiting for crawler thread to finish (this might take around 30s)")
    stop_request = True
    signal.signal(signal.SIGINT, original_sigint)
    try:
        if not wait_for_event(finished, 60):
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

#console = logging.StreamHandler()
def init_logging():
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
                                  '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)


if __name__ == '__main__':
    init_logging()
    init_dropbox()

    load_data()
    Thread(target=crawl).start()

    # print('polling for updates..')

    original_sigint = signal.signal(signal.SIGINT, exit_handler)
    if os.name != 'nt':
        signal.pause()
    else:
        try:
            while True:
                time.sleep(10)
        except KeyboardInterrupt:
            pass