# -*- coding: utf-8 -*-
#
import pytest
import elliptics

from hamcrest import assert_that, equal_to, calling, raises, is_
import sys

import test_helper.elliptics_testhelper as et
from test_helper.elliptics_testhelper import key_and_data, timestamp, user_flags, client, nodes
import test_helper.utils as utils
from test_helper.matchers import elliptics_result_with

from testcases import size_type_list, offset_type_positive_list, offset_type_negative_list, \
    offset_and_size_types_positive_list, offset_and_size_types_negative_list

@pytest.mark.readtest
def test_write_read(client, key_and_data, timestamp, user_flags):
    """ Testing basic writing and reading
    """
    key, data = key_and_data
    client.set_user_flags(user_flags)
    
    client.write_data_sync(key, data)
    result = client.read_data_sync(key).pop()

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.readtest
@pytest.mark.parametrize("size_type", size_type_list)
def test_read_with_size(client, size_type, key_and_data, timestamp, user_flags):
    """ Testing read command (with size)
    """
    key, data = key_and_data
    size = et.SizeGetter[size_type](len(data))
    client.set_user_flags(user_flags)

    client.write_data_sync(key, data)
    result = client.read_data_sync(key, size=size).pop()

    if not size:
        size = len(data)
    data = data[:size]

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))
    
@pytest.mark.readtest
@pytest.mark.parametrize("offset_type", offset_type_positive_list)
def test_read_with_offset_positive(client, offset_type, key_and_data, timestamp, user_flags):
    """ Testing read command (with offset) (positive test cases)
    """
    key, data = key_and_data
    offset = et.OffsetReadGetter[offset_type](len(data))
    client.set_user_flags(user_flags)

    client.write_data_sync(key, data)
    
    result = client.read_data_sync(key, offset=offset).pop()

    data = data[offset:]

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.readtest
@pytest.mark.parametrize("offset_type", offset_type_negative_list)
def test_read_with_offset_negative(client, offset_type, key_and_data):
    """ Testing read command (with offset) (negative test cases)
    """
    key, data = key_and_data
    offset = et.OffsetReadGetter[offset_type](len(data))

    client.write_data_sync(key, data)

    assert_that(calling(client.read_data_sync).with_args(key, offset=offset),
                raises(Exception, client.error_info.WrongArguments))

@pytest.mark.readtest
@pytest.mark.parametrize(("offset_type", "size_type"), offset_and_size_types_positive_list)
def test_read_with_offset_and_size_positive(client, offset_type, size_type,
                                            key_and_data, timestamp, user_flags):
    """ Testing read command (with offset and size) (positive test cases)
    """
    key, data = key_and_data
    offset = et.OffsetReadGetter[offset_type](len(data))
    size = et.SizeGetter[size_type](len(data), offset)
    client.set_user_flags(user_flags)

    client.write_data_sync(key, data)
    result = client.read_data_sync(key, offset=offset, size=size).pop()

    if not size:
        size = len(data)
    data = data[offset:offset+size]

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))

@pytest.mark.readtest
@pytest.mark.parametrize(("offset_type", "size_type"), offset_and_size_types_negative_list)
def test_read_with_offset_and_size_negative(client, offset_type, size_type, key_and_data):
    """ Testing read command (with offset and size) (negative test cases)
    """
    key, data = key_and_data
    offset = et.OffsetReadGetter[offset_type](len(data))
    size = et.SizeGetter[size_type](len(data), offset)

    client.write_data_sync(key, data)

    assert_that(calling(client.read_data_sync).with_args(key, offset=offset, size=size),
                raises(Exception, client.error_info.WrongArguments))
