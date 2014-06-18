# -*- coding: utf-8 -*-

import hashlib
import random

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
