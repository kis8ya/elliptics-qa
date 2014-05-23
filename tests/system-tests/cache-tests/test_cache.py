import pytest
import time
import random

import elliptics

from hamcrest import assert_that, less_than_or_equal_to

import elliptics_testhelper as et

from utils import get_key_and_data, MB
from logging_tests import logger

@pytest.fixture(scope='function')
def client():
    """Prepares elliptics.Session object with elliptics.io_flags.cache
    """
    nodes = et.EllipticsTestHelper.get_nodes_from_args(pytest.config.getoption("node"))
    client = et.EllipticsTestHelper(nodes=nodes, wait_timeout=3, check_timeout=30)
    client.set_ioflags(elliptics.io_flags.cache)
    return client

def test_cache_overhead(client):
    """Testing that elliptics will process commands just in time
    when there is a cache overhead
    """
    count = 100000
    logger.info("\n0/{0}".format(count))
    for i in xrange(count):
        key = str(i)
        client.write_data_sync(key, '?')
        logger.info('\r{0}/{1}'.format(i + 1, count))

@pytest.fixture
def requests_number(pytestconfig):
    return pytestconfig.option.requests_number

def time_requests(client, requests_count, hot_keys, cold_keys):
    start_time = time.time()

    hot_requests_percentage = 0.8
    for i in xrange(requests_count):
        if random.random() < hot_requests_percentage:
            key = random.choice(hot_keys)
        else:
            key = random.choice(cold_keys)
        client.read_data_sync(key)

    result_time = time.time() - start_time

    return result_time

def test_cache_lru(client, requests_number):
    """Testing that data requests will take not too much more time
    after stuffing cache with unused data
    """
    hot_keys_count = 5000
    cold_keys_count = 45000
    hot_keys = map(str, xrange(hot_keys_count))
    cold_keys = map(str, xrange(hot_keys_count, cold_keys_count + hot_keys_count))

    for k in hot_keys + cold_keys:
        client.write_data_sync(k, '?')

    time_before = time_requests(client, requests_number, hot_keys, cold_keys)

    # Stuffing cache
    key, data = get_key_and_data(5*MB, randomize_len=False)
    client.write_data_sync(key, data)

    time_after = time_requests(client, requests_number, hot_keys, cold_keys)

    diff_time = time_after - time_before
    allowed_overhead_time = time_before * 0.1
    assert_that(diff_time, less_than_or_equal_to(allowed_overhead_time))
