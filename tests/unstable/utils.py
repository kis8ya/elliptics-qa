import inspect
import random
import json
import socket

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


def get_good_keys(session, good_keys_path, good_files_number, file_size, indexes):
    """Returns "good" keys.

    If file with good keys was specified then it will return keys from the file.
    Otherwise it will write keys and return these keys.

    """
    if good_keys_path:
        good_keys = _load_keys_from_file(good_keys_path)
    else:
        good_keys = _write_files_with_indexes(session,
                                              good_files_number,
                                              file_size,
                                              indexes)
    return good_keys


def get_bad_keys(session, bad_keys_path, bad_files_number,
                 file_size, indexes, dropped_groups):
    """Returns "bad" keys.

    If file with bad keys was specified then it will return keys from the file.
    Otherwise it will write keys and return these keys.

    """
    if bad_keys_path:
        bad_keys = _load_keys_from_file(bad_keys_path)
    else:
        restricted_session = session.clone()
        # "Bad" keys will be written in all groups except groups from dropped_groups
        restricted_session.set_groups([g for g in restricted_session.groups
                                       if g not in dropped_groups])
        bad_keys = _write_files_with_indexes(restricted_session,
                                             bad_files_number,
                                             file_size,
                                             indexes)
    return bad_keys


def get_broken_keys(session, broken_keys_path, broken_files_number,
                    file_size, indexes, dropped_groups):
    """Returns "broken" keys.

    If file with broken keys was specified then it will return keys from the file.
    Otherwise it will write keys and return these keys.

    """
    if broken_keys_path:
        broken_keys = _load_keys_from_file(broken_keys_path)
    else:
        restricted_session = session.clone()
        # "Broken" keys will be written in all groups except groups from dropped_groups
        restricted_session.set_groups([g for g in restricted_session.groups
                                       if g not in dropped_groups])
        broken_keys = _write_files_with_indexes(restricted_session,
                                                broken_files_number,
                                                file_size,
                                                indexes)
    return broken_keys


def get_expected_keys(recovery, group, index):
    """Returns a list of all keys for the index that should be available in the group."""
    expected_keys = [key for key, key_indexes in recovery["keys"]["good"].items()
                     if index in key_indexes]
    expected_keys.extend([key for key, key_indexes in recovery["keys"]["bad"].items()
                          if index in key_indexes])
    if group not in recovery["dropped_groups"]:
        expected_keys.extend([key for key, key_indexes in recovery["keys"]["broken"].items()
                              if index in key_indexes])
    return expected_keys


class Range(object):
    """Range of elliptics id."""
    def __init__(self, couple):
        self.left = couple[0]
        self.right = couple[1]

    def __contains__(self, item):
        return self.left <= item < self.right


class Ranges(object):
    """Ranges for the following check: *is these ranges contains specified elliptics.Id*."""
    def __init__(self, ranges_list):
        self.ranges = []
        for couple in ranges_list:
            self.ranges.append(Range(couple))

    def __contains__(self, item):
        return any(item in r for r in self.ranges)


def get_ranges_by_session(session, nodes):
    """Returns ranges of elliptics id as dictionary:

    {
      repr(server-1): <ranges of elliptics id for server-1>,
      ...
    }

    """
    ranges = {}
    for address in session.routes.addresses():
        filtered_hostname = [repr(node) for node in nodes
                             if socket.gethostbyname(node.host) == address.host and
                             node.port == address.port]
        ranges[filtered_hostname[0]] = Ranges(session.routes.get_address_ranges(address))
    return ranges


def dump_keys_to_file(session, keys, file_path):
    """Writes id of given keys to a dump file."""
    ids = [str(session.transform(k)) for k in keys]
    with open(file_path, "w") as dump_file:
        dump_file.write("\n".join(ids))
