# -*- coding: utf-8 -*-

import hashlib
import random
import inspect
import importlib                                                                          

from os import urandom

KB = 1 << 10
MB = 1 << 20

MIN_LENGTH = 10*KB
MAX_LENGTH = 10*MB
USER_FLAGS_MAX = 2**64 - 1

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
    for i in range(list_size):
        data_list.append(get_data())
    key = get_sha1(''.join(data_list))

    return key, data_list

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

def _get_testcases_items(testcases_module_name):
    testcases_module = importlib.import_module(testcases_module_name)
    for func_name, func in inspect.getmembers(testcases_module, inspect.isfunction):
        if inspect.getmodule(func) == testcases_module:
            yield func_name, func

def get_testcases(testcases_module_name):
    return [testcase for _, testcase in _get_testcases_items(testcases_module_name)]

def get_testcases_names(testcases_module_name):
    return [testcase_name for testcase_name, _ in _get_testcases_items(testcases_module_name)]
