"""Utils module for recovery tests for working with keys."""

import random
import json
import copy
import socket

import recovery.utils.testrecovery

from test_helper.logging_tests import logger
from test_helper.utils import get_key_and_data


# index data format
INDEX_DATA_TEMPLATE = "{}_{}"


def _write_files(key_writer, files_number, file_size):
    """Writes files with preset session and specified files parameters: numbers and size."""
    logger.info("Started writing {} files\n".format(files_number))

    keys = set()
    for i in xrange(files_number):
        key = key_writer.write(file_size)
        keys.add(key)
        logger.info("\r{}/{}".format(i + 1, files_number))

    logger.info("\nFinished writing files\n")

    return keys


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
        index_data = [INDEX_DATA_TEMPLATE.format(key, index) for index in key_indexes]

        session.set_indexes(key, key_indexes, index_data).wait()
        keys_indexes[key] = set(key_indexes)

        logger.info("\r{}/{}".format(i + 1, len(keys)))

    logger.info("\nFinished setting indexes\n")

    return keys_indexes


def _load_keys_from_file(path):
    """Returns keys from specified file."""
    keys = json.load(open(path))
    keys = {str(key): {str(index) for index in indexes} for key, indexes in keys.items()}
    return keys


def _write_files_with_indexes(key_writer, session, files_number, file_size, indexes):
    """Writes files then sets indexes for these files."""
    keys = _write_files(key_writer, files_number, file_size)
    keys_with_indexes = _set_indexes(session, keys, indexes)
    return keys_with_indexes


def _get_keys(keys_path, key_writer, session, files_number, file_size, indexes):
    """Returns a dictionary with keys and their indexes."""
    if keys_path:
        keys = _load_keys_from_file(keys_path)
    else:
        keys = _write_files_with_indexes(key_writer, session, files_number, file_size, indexes)
    return keys


class KeyWriter(object):
    """Default keys writer to elliptics cluster."""
    def __init__(self, session):
        self.session = session

    def write(self, file_size):
        """Writes a key to elliptics cluster with specified files size."""
        key, data = get_key_and_data(file_size, randomize_len=False)
        self.session.write_data(key, data).wait()
        return key


class HashRingInconsistentKeyWriter(object):
    """Inconsistent keys writer to elliptics cluster.

    It writes inconsistent keys for hash ring, which is specified by `session`.

    """
    def __init__(self, session, full_routes):
        self.restricted_session = session
        self.full_routes = full_routes

    def _key_in_restricted_nodes(self, key):
        key_id = self.restricted_session.transform(key)
        # There is only one group in our test cluster (accordingly, only one route)
        key_route = self.full_routes.get_id_routes(key_id)[0]
        key_addr = key_route[0]
        return key_addr in self.restricted_session.routes.addresses()

    def write(self, file_size):
        """Writes an inconsistent key to elliptics cluster.

        It writes a key only when this key will be inconsistent for hash ring from
        `restircted_session` (it has only one group).
        
        """
        key, data = get_key_and_data(file_size, randomize_len=False)
        while self._key_in_restricted_nodes(key):
            key, data = get_key_and_data(file_size, randomize_len=False)

        self.restricted_session.write_data(key, data).wait()
        return key


def get_keys_for_dc(options, session, dropped_groups, indexes):
    """Returns keys for recovery-dc tests."""
    key_writer = KeyWriter(session)
    consistent_keys = _get_keys(options.consistent_keys_path,
                                key_writer,
                                session,
                                options.consistent_files_number,
                                options.file_size,
                                indexes)

    # Restrict session to write keys in all groups except groups from dropped_groups
    full_groups = session.groups
    session.groups = [group for group in session.groups
                      if group not in dropped_groups]

    key_writer = KeyWriter(session)
    inconsistent_keys = _get_keys(options.inconsistent_keys_path,
                                  key_writer,
                                  session,
                                  options.inconsistent_files_number,
                                  options.file_size,
                                  indexes)

    # Reset session's groups
    session.groups = full_groups

    return {
        "consistent": consistent_keys,
        "inconsistent": inconsistent_keys
    }


def get_keys_for_merge(options, session, dropped_nodes, indexes):
    """Returns keys for recovery-merge tests."""
    key_writer = KeyWriter(session)
    consistent_keys = _get_keys(options.consistent_keys_path,
                                key_writer,
                                session,
                                options.consistent_files_number,
                                options.file_size,
                                indexes)

    # Restrict elliptics cluster: disable all backends in specified nodes
    full_routes = copy.deepcopy(session.routes)
    recovery.utils.testrecovery.disable_all_backends(session, dropped_nodes, options.backends_number)

    key_writer = HashRingInconsistentKeyWriter(session, full_routes)
    inconsistent_keys = _get_keys(options.inconsistent_keys_path,
                                  key_writer,
                                  session,
                                  options.inconsistent_files_number,
                                  options.file_size,
                                  indexes)

    # Enable all disable backends
    recovery.utils.testrecovery.enable_all_backends(session, dropped_nodes, options.backends_number)

    return {
        "consistent": consistent_keys,
        "inconsistent": inconsistent_keys
    }


def dump_keys_to_file(session, keys, file_path):
    """Writes IDs of given keys to a dump file."""
    ids = [str(session.transform(key)) for key in keys]
    with open(file_path, "w") as dump_file:
        dump_file.write("\n".join(ids))


def split_node_keys(session, keys, node):
    """Splits keys wich should belong to specified node."""
    node_address = socket.gethostbyname(node.host)

    node_keys = {}
    rest_keys = {}
    for key, key_indexes in keys.items():
        key_id = session.transform(key)
        key_node = session.lookup_address(key_id, node.group)

        if key_node.host == node_address and \
           key_node.port == node.port:
            node_keys[key] = key_indexes
        else:
            rest_keys[key] = key_indexes

    return (node_keys, rest_keys)


def split_keys_in_percentage(keys, percentage):
    """Splits keys in percentage."""
    inconsistent_keys_number = int(len(keys) * percentage)
    inconsistent_keys = dict(keys.items()[:inconsistent_keys_number])
    recovered_keys = dict(keys.items()[inconsistent_keys_number:])
    return (recovered_keys, inconsistent_keys)
