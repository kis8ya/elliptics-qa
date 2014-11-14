# -*- coding: utf-8 -*-

import hashlib
import random
import inspect
import importlib
import elliptics
import os.path
import os
import socket

from os import urandom


KB = 1 << 10
MB = 1 << 20


MIN_LENGTH = 10*KB
MAX_LENGTH = 10*MB
USER_FLAGS_MAX = 2**64 - 1


class Client(object):
    """Clients object for testing needs."""
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def __repr__(self):
        return "Client({host}, {port})".format(**self.__dict__)


class Node(object):
    def __init__(self, host, port, group):
        self.host = host
        self.port = int(port)
        self.group = int(group)

    def __repr__(self):
        return "Node({0}, {1}, {2})".format(self.host, self.port, self.group)


def get_nodes_from_option(node_option):
    """Returns list of nodes from command line options."""
    nodes = [Node(*node_str.split(':'))
             for node_str in node_option]
    return nodes


def create_session(nodes, check_timeout=None, wait_timeout=None, test_name="test"):
    """Returns elliptics session."""
    log_dir_path = "/var/log/elliptics_testing"
    log_path = "{}/client-{}.log".format(log_dir_path, test_name)
    if not os.path.exists(log_dir_path):
        os.makedirs(log_dir_path)
    elog = elliptics.Logger(log_path, elliptics.log_level.debug)

    config = elliptics.Config()
    if wait_timeout:
        config.wait_timeout = wait_timeout
    if check_timeout:
        config.check_timeout = check_timeout

    client_node = elliptics.Node(elog, config)
    addresses = [elliptics.Address(node.host, node.port, socket.AF_INET)
                 for node in nodes]
    client_node.add_remotes(addresses)

    elliptics_session = elliptics.Session(client_node)

    # Get uniq groups from nodes list
    groups = list({node.group for node in nodes})
    elliptics_session.groups = groups

    return elliptics_session


def get_data(size=MAX_LENGTH, randomize_len=True):
    """ Returns a string of random bytes
    """
    if randomize_len:
        size = random.randint(MIN_LENGTH, size)
    data = urandom(size)
    return data


def get_sha1(data):
    m = hashlib.sha1()
    m.update(data)
    return m.hexdigest()


def get_key_and_data(size=MAX_LENGTH, randomize_len=True):
    data = get_data(size, randomize_len)
    key = get_sha1(data)
    return (key, data)


def get_key_and_data_list(list_size=3):
    data_list = []
    for _ in range(list_size):
        data_list.append(get_data())
    key = get_sha1(''.join(data_list))

    return key, data_list


def _get_testcases_items(testcases_module_name):
    testcases_module = importlib.import_module(testcases_module_name)
    for func_name, func in inspect.getmembers(testcases_module, inspect.isfunction):
        if inspect.getmodule(func) == testcases_module:
            yield func_name, func


def get_testcases(testcases_module_name):
    return [testcase for _, testcase in _get_testcases_items(testcases_module_name)]


def get_testcases_names(testcases_module_name):
    return [testcase_name for testcase_name, _ in _get_testcases_items(testcases_module_name)]
