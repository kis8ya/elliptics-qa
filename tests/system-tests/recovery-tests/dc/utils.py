import random
import json

from test_helper.logging_tests import logger
from test_helper.utils import get_key_and_data


def _write_files(session, files_number, file_size):
    """Writes files with preset session and specified files parameters: numbers and size."""
    logger.info("Started writing {} files\n".format(files_number))

    keys = set()
    for i in xrange(files_number):
        key, data = get_key_and_data(file_size, randomize_len=False)

        session.write_data(key, data).wait()
        keys.add(key)
        logger.info("\r{}/{}".format(i + 1, files_number))

    logger.info("\nFinished writing files\n")

    return keys


def index_data_format(key, index):
    """Returns index data for given key and index."""
    return "{}_{}".format(key, index)


def _set_indexes(session, keys, indexes):
    """Prepares secondary indexes.

    Sets randomly chosen indexes for a given list of keys and
    returns a dictionary with information about these indexes.

    """
    logger.info("Started setting indexes for {} keys...\n".format(len(keys)))

    keys_indexes = {}
    for i, key in enumerate(keys):
        indexes_count = random.randint(0, len(indexes))
        key_indexes = random.sample(indexes, indexes_count)
        index_data = [index_data_format(key, index) for index in key_indexes]

        session.set_indexes(key, key_indexes, index_data).wait()
        keys_indexes[key] = set(key_indexes)

        logger.info("\r{}/{}".format(i + 1, len(keys)))

    logger.info("\nFinished setting indexes\n")

    return keys_indexes


def _load_keys_from_file(path):
    """Returns keys from specified file."""
    keys = json.load(open(path))
    keys = {str(k) for k in keys}
    return keys


def _write_files_with_indexes(session, files_number, file_size, indexes):
    """Writes files then sets indexes for these files."""
    keys = _write_files(session, files_number, file_size)
    keys = _set_indexes(session, keys, indexes)
    return keys


def _get_consistent_keys(options, session, indexes):
    if options.consistent_keys_path:
        consistent_keys = _load_keys_from_file(options.consistent_keys_path)
    else:
        consistent_keys = _write_files_with_indexes(session,
                                                    options.consistent_files_number,
                                                    options.file_size,
                                                    indexes)
    return consistent_keys


def _get_inconsistent_keys(options, session, dropped_groups, indexes):
    if options.inconsistent_keys_path:
        inconsistent_keys = _load_keys_from_file(options.inconsistent_keys_path)
    else:
        restricted_session = session.clone()
        # These keys will be written in all groups except groups from dropped_groups
        restricted_session.set_groups([group for group in restricted_session.groups
                                       if group not in dropped_groups])
        inconsistent_keys = _write_files_with_indexes(restricted_session,
                                                      options.inconsistent_files_number,
                                                      options.file_size,
                                                      indexes)
    return inconsistent_keys


def get_keys(options, session, dropped_groups, indexes):
    keys = {
        "consistent": _get_consistent_keys(options, session, indexes),
        "inconsistent": _get_inconsistent_keys(options, session, dropped_groups, indexes)
    }
    return keys


def get_expected_keys(recovery, group, index, dropped_groups):
    """Returns a list of all keys for the index that should be available in the group."""
    expected_keys = [key for key, key_indexes in recovery["keys"]["consistent"].items()
                     if index in key_indexes]
    # add recovered keys if their indexes were recovered or we are checking indexes in group
    # which was available and had indexes before recovery operation
    if recovery["recovery_indexes"] or group not in dropped_groups:
        expected_keys.extend([key for key, key_indexes in recovery["keys"]["recovered"].items()
                              if index in key_indexes])
    if group not in dropped_groups:
        expected_keys.extend([key for key, key_indexes in recovery["keys"]["inconsistent"].items()
                              if index in key_indexes])
    return expected_keys


def dump_keys_to_file(session, keys, file_path):
    """Writes id of given keys to a dump file."""
    ids = [str(session.transform(k)) for k in keys]
    with open(file_path, "w") as dump_file:
        dump_file.write("\n".join(ids))
