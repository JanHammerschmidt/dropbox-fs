import logging
import os
import time

log = logging.getLogger(__name__)


def remove_from_dict_case_insensitive(dict, key):
    key_lower = key.lower()
    existing_key = next((k for k in dict.keys() if k.lower() == key_lower), None)
    if existing_key is not None:
        log.debug('change {} to {}'.format(existing_key, key))
        del dict[existing_key]


def wait_for_event(event, timeout_seconds):
    if os.name != 'nt':
        return event.wait(timeout_seconds)
    t0 = time.time()
    while (time.time() - t0) < timeout_seconds:
        if event.is_set():
            return True
    return False
