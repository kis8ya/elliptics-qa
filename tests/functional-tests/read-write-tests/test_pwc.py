# -*- coding: utf-8 -*-
#
import pytest
import random
import elliptics

from hamcrest import assert_that, equal_to, calling, raises, has_property, has_length, is_

import elliptics_testhelper as et
from elliptics_testhelper import key_and_data, timestamp, user_flags, client, nodes
import utils
from utils import elliptics_result_with

def get_length(data):
    return sum(len(i) for i in data)

@pytest.mark.pwctest
def test_prepare_write_commit(client, timestamp, user_flags):
    """ Testing basic prepare-write-commit writing
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)
    client.set_user_flags(user_flags)

    client.write_prepare_sync(key, data[0], 0, data_len)
    client.write_plain_sync(key, data[1], len(data[0]))
    client.write_commit_sync(key, data[2], len(data[0]) + len(data[1]), data_len)

    result = client.read_data_sync(key).pop()
    data = ''.join(data)

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

#TODO: add this test case back (it's was excluded temporary for some reasons)
#@pytest.mark.pwctest
def test_pwc_inaccessibility(client, timestamp, user_flags):
    """ Testing that data is inaccessible after write_prepare and write_plain
    (prepare-write-commit scheme)
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)
    client.set_user_flags(user_flags)

    client.write_prepare_sync(key, data[0], 0, data_len)
    client.checking_inaccessibility(key, data_len)
    client.write_plain_sync(key, data[1], len(data[0]))
    client.checking_inaccessibility(key, data_len)
    client.write_commit_sync(key, data[2], len(data[0]) + len(data[1]), data_len)

    result = client.read_data_sync(key).pop()
    data = ''.join(data)

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_prepare_write_write_commit(client, timestamp, user_flags):
    """ Testing prepare-write-commit (with multiple write_plain)
    """
    key, data = utils.get_key_and_data_list(list_size=random.randint(4, 10))
    data_len = get_length(data)
    client.set_user_flags(user_flags)

    client.write_prepare_sync(key, data[0], 0, data_len)
    for p in xrange(1, len(data) - 1):
        client.write_plain_sync(key, data[p], get_length(data[:p]))
    client.write_commit_sync(key, data[-1], data_len - len(data[-1]), data_len)

    result = client.read_data_sync(key).pop()
    data = ''.join(data)
    
    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_prepare_commit(client, timestamp, user_flags):
    """ Testing prepare-write-commit (without write_plain)
    """
    key, data = utils.get_key_and_data_list(list_size=2)
    data_len = get_length(data)
    client.set_user_flags(user_flags)

    client.write_prepare_sync(key, data[0], 0, data_len)
    client.write_commit_sync(key, data[1], len(data[0]), data_len)

    result = client.read_data_sync(key).pop()
    data = ''.join(data)
    
    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_commit(client, key_and_data):
    """ Testing that write_commit without write_prepare will raise the elliptics.Error
    """
    key, data = key_and_data

    assert_that(calling(client.write_commit_sync).with_args(key, data, 0, len(data)),
                raises(elliptics.Error, client.error_info.NotExists))

@pytest.mark.pwctest
def test_pwc_not_entire_data(client, timestamp, user_flags):
    """ Testing prepare-write-commit
    when prepare_size and commit_size > data length
    """
    key, data = utils.get_key_and_data_list()
    additional_length = random.randint(1, utils.MAX_LENGTH >> 1)
    data_len = get_length(data) + additional_length
    client.set_user_flags(user_flags)

    client.write_prepare_sync(key, data[0], 0, data_len)
    client.write_plain_sync(key, data[1], len(data[0]))
    client.write_commit_sync(key, data[2], len(data[0]) + len(data[1]), data_len)

    result = client.read_data_sync(key).pop()
    
    assert_that(result, has_property('data', has_length(data_len)))

    data = ''.join(data) + str(result.data)[data_len-additional_length:]

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_pwc_more_than_prepared(client, key_and_data):
    """ Testing prepare-write-commit
    when data length > prepare_size
    """
    key, data = key_and_data
    data_len = len(data) - 1

    assert_that(calling(client.write_prepare_sync).with_args(key, data, 0, data_len),
                raises(elliptics.Error, client.error_info.WrongArguments))

@pytest.mark.pwctest
def test_pwc_psize_more_than_csize(client, timestamp, user_flags):
    """ Testing prepare-write-commit
    when data length < prepare_size > commit_size
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)
    client.set_user_flags(user_flags)

    add_len = random.randint(1, utils.MAX_LENGTH >> 1)
    client.write_prepare_sync(key, data[0], 0, data_len + add_len)
    client.write_plain_sync(key, data[1], len(data[0]))
    client.write_commit_sync(key, data[2], len(data[0]) + len(data[1]), data_len)

    result = client.read_data_sync(key).pop()
    
    data = ''.join(data)

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_pwc_psize_more_than_csize_negative(client, timestamp, user_flags):
    """ Testing prepare-write-commit
    when prepare_size > commit_size < data length
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)
    client.set_user_flags(user_flags)

    sub_len = random.randint(1, data_len - 1)
    client.write_prepare_sync(key, data[0], 0, data_len)
    client.write_plain_sync(key, data[1], len(data[0]))
    commit_size = data_len - sub_len
    client.write_commit_sync(key, data[2], len(data[0]) + len(data[1]), commit_size)

    result = client.read_data_sync(key).pop()

    data = ''.join(data)[:commit_size]

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_pwc_psize_less_than_plainsize_negative(client):
    """ Testing prepare-write-commit
    when prepare_size < data_size (write_plain)
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)

    sub_len = random.randint(len(data[2]) + 1, len(data[1]) + len(data[2]) - 1)
    client.write_prepare_sync(key, data[0], 0, data_len - sub_len)

    assert_that(calling(client.write_plain_sync).with_args(key, data[1], len(data[0])),
                raises(elliptics.Error, client.error_info.WrongArguments))

@pytest.mark.pwctest
def test_pwc_psize_less_than_csize_negative(client):
    """ Testing prepare-write-commit
    when prepare_size < commit_size
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)

    sub_len = random.randint(1, len(data[2]) - 1)
    client.write_prepare_sync(key, data[0], 0, data_len - sub_len)
    client.write_plain_sync(key, data[1], len(data[0]))

    assert_that(calling(client.write_commit_sync).with_args(key, data[2], len(data[0]) + len(data[1]), data_len),
                raises(elliptics.Error, client.error_info.WrongArguments))

@pytest.mark.pwctest
def test_pwc_psize_less_than_csize(client):
    """ Testing prepare-write-commit
    when prepare_size < commit_size > data length
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)

    add_len = random.randint(1, utils.MAX_LENGTH)
    client.write_prepare_sync(key, data[0], 0, data_len)
    client.write_plain_sync(key, data[1], len(data[0]))

    assert_that(calling(client.write_commit_sync).with_args(key, data[2], len(data[0]) + len(data[1]), data_len + add_len),
                raises(elliptics.Error))

@pytest.mark.pwctest
def test_pwc_null_psize(client, timestamp, user_flags):
    """ Testing prepare-write-commit
    when prepare_size = 0
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)
    client.set_user_flags(user_flags)

    assert_that(calling(client.write_prepare_sync).with_args(key, data[0], 0, 0),
                raises(elliptics.Error, client.error_info.WrongArguments))

@pytest.mark.pwctest
def test_pwc_null_csize(client, timestamp, user_flags):
    """ Testing prepare-write-commit
    when commit_size = 0
    """
    key, data = utils.get_key_and_data_list()
    data_len = get_length(data)
    client.set_user_flags(user_flags)

    client.write_prepare_sync(key, data[0], 0, data_len)
    client.write_plain_sync(key, data[1], len(data[0]))
    client.write_commit_sync(key, data[2], len(data[0]) + len(data[1]), 0)

    result = client.read_data_sync(key).pop()
    
    data = ''

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.pwctest
def test_pwc_prepare_less_than_data1(client, key_and_data):
    """ Testing that write_prepare with data length > prepare_size will raise the elliptics.Error
    """
    key, data = key_and_data

    assert_that(calling(client.write_prepare_sync).with_args(key, data, 0, len(data) - 1),
                raises(Exception, client.error_info.WrongArguments))
