import logging

log = logging.getLogger(__name__)


def remove_from_dict_case_insensitive(dict, key):
    key_lower = key.lower()
    existing_key = next((k for k in dict.keys() if k.lower() == key_lower), None)
    if existing_key is not None:
        log.debug('change {} to {}'.format(existing_key, key))
        del dict[existing_key]
