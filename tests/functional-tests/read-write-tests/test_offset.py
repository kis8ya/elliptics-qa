# -*- coding: utf-8 -*-
#
import pytest
import elliptics

from hamcrest import assert_that, equal_to, has_property, has_length, is_

import elliptics_testhelper as et
from elliptics_testhelper import key_and_data, timestamp, user_flags

import utils
from utils import elliptics_result_with
from testcases import offset_and_chunksize_types_list, \
                      write_offset_type_and_overriding_list

config = pytest.config

WAIT_TIMEOUT = config.getoption("wait_timeout")
CHECK_TIMEOUT = config.getoption("check_timeout")

nodes = et.EllipticsTestHelper.get_nodes_from_args(config.getoption("node"))

testhelper = et.EllipticsTestHelper(nodes=nodes,
                                    wait_timeout=WAIT_TIMEOUT,
                                    check_timeout=CHECK_TIMEOUT)

@pytest.mark.offsettest
@pytest.mark.parametrize(("offset_type", "override"), write_offset_type_and_overriding_list)
def test_offset(offset_type, override, key_and_data, timestamp, user_flags):
    """ Testing write command (with offset)
    """
    key, data = key_and_data
    testhelper.set_user_flags(user_flags)

    offset = et.OffsetWriteGetter[offset_type](len(data))

    if override:
        offset_type = "APPENDING"
    offset_data = utils.get_data(et.OffsetDataGetter[offset_type](len(data), offset), randomize_len=False)

    testhelper.write_data_sync(key, data)
    testhelper.write_data_sync(key, offset_data, offset)
    # Checking written data
    result = testhelper.read_data_sync(key).pop()
    # that data length is correct
    assert_that(result, has_property('data', has_length(offset + len(offset_data))))

    sample = data[:offset] + str(result.data)[len(data):offset] + offset_data

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=sample)))

@pytest.mark.offsettest
@pytest.mark.parametrize(("offset_type", "chunk_size_type"), offset_and_chunksize_types_list)
def test_offset_and_chunksize(offset_type, chunk_size_type,
                              key_and_data, timestamp, user_flags):
    """ Testing write command (with offset and chunk_size) (positive test cases)
    """
    key, data = key_and_data
    testhelper.set_user_flags(user_flags)
    offset = et.OffsetReadGetter[offset_type](len(data))
    chunk_size = et.ChunkSizeGetter[chunk_size_type](len(data))

    testhelper.write_data_sync(key, data, offset, chunk_size)

    result = testhelper.read_data_sync(key, offset).pop()

    assert_that(result, is_(elliptics_result_with(error_code=0,
                                                  timestamp=timestamp,
                                                  user_flags=user_flags,
                                                  data=data)))
