import pytest
import time
import random

from hamcrest import assert_that, less_than_or_equal_to

from test_helper.utils import get_key_and_data, MB
from test_helper.logging_tests import logger


def time_requests(client, requests_count, hot_keys, cold_keys):
    """Returns time which was spent to process numerous requests."""
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


def test_cache_lru(pytestconfig, client):
    """Testing that data requests will take not too much more time
    after stuffing cache with unused data.
    """
    requests_number = pytestconfig.option.requests_number
    allowed_time_diff_rate = pytestconfig.option.allowed_time_diff_rate
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
    allowed_overhead_time = time_before * allowed_time_diff_rate
    assert_that(diff_time, less_than_or_equal_to(allowed_overhead_time))
