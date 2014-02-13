# -*- coding: utf-8 -*-
#
import hashlib
import random

from hamcrest.core.base_matcher import BaseMatcher
from hamcrest.core.helpers.hasmethod import hasmethod
from hamcrest import has_properties, has_property, equal_to, greater_than_or_equal_to

from os import urandom, environ

KB = 1 << 10
MB = 1 << 20

MAX_LENGTH = 10*MB
USER_FLAGS_MAX = 2**64 - 1

def get_data(size=MAX_LENGTH, randomize_len=True):
    """ Возвращает последовательность из случайно-сгенерированных байтов
    """
    if randomize_len:
        size = random.randint(10, size)
    data = urandom(size)
    return data

def get_sha1(data):
    m = hashlib.sha1()
    m.update(data)
    return m.hexdigest()

def get_key_and_data_list(list_size=3):
    data_list = []
    for i in range(list_size):
        data_list.append(get_data())
    key = get_sha1(''.join(data_list))

    return key, data_list

class WithSameSha1As(BaseMatcher):
    def __init__(self, data):
        self.expected_sha1 = get_sha1(data)

    def matches(self, item, mismatch_description=None):
        result_sha1 = get_sha1(str(item))
        return result_sha1 == self.expected_sha1

    def describe_to(self, description):
        description.append_text('by sha1 to ') \
                   .append_text(self.expected_sha1)

    def describe_mismatch(self, item, mismatch_description):
        mismatch_description.append_text(get_sha1(str(item)))

def with_same_sha1_as(data):
    return WithSameSha1As(data)

def elliptics_result_with(error_code, timestamp, user_flags, data):
    """ Матчер для проверки асинхронного результат elliptics'а на то, что
    error.code равен нулю
    timestamp >= timestamp'у до выполнения операции
    user_flags установлены в то значение, в которое они были установлены на момент записи
    """
    return has_properties('error', has_property('code', equal_to(error_code)),
                          'timestamp', greater_than_or_equal_to(timestamp),
                          'user_flags', equal_to(user_flags),
                          'data', with_same_sha1_as(data))
