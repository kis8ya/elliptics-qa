# -*- coding: utf-8 -*-

import elliptics
import hashlib
import random
import pytest
import os

from collections import defaultdict

import elliptics_testhelper as et

from utils import get_key_and_data, get_sha1

MIN_SIZE = 100
MAX_SIZE = 1000
config = pytest.config
BATCH_SIZE = config.getoption('batch_size') 
BATCH_NUMBER = config.getoption('batch_number') 
CHECK_TIMEOUT = config.getoption('check_timeout') 
WAIT_TIMEOUT = config.getoption('wait_timeout') 
nodes = et.EllipticsTestHelper.get_nodes_from_args(config.getoption("node"))

s = et.EllipticsTestHelper(nodes=nodes, wait_timeout=WAIT_TIMEOUT, check_timeout=CHECK_TIMEOUT)
timestamp = elliptics.Time.now()

ids_batches = []
keys = set()

@pytest.fixture()
def put_keys(request):
    for i in xrange(BATCH_NUMBER):
        ids = []
        results = []
        # Generate and asynchronous writing a bunch of data
        for j in xrange(BATCH_SIZE):
            size = random.randint(MIN_SIZE, MAX_SIZE)
            key, data = get_key_and_data(size, randomize_len=False)
            elliptics_id = elliptics.Id(key)
            ids.append(elliptics_id)
            result = s.write_data(elliptics_id, data)
            results.append(result)
        ids_batches.append(ids)
        for result in results:
            result.get()

def test_elliptics(put_keys):
    failures = defaultdict(list)
    for ids in ids_batches:
        for result in s.bulk_read(ids):
            data = str(result.data)
            elliptics_id = result.id
            sha1 = get_sha1(data)
            actual_elliptics_id = s.transform(elliptics.Id(sha1))
            if elliptics_id != actual_elliptics_id:
                failures[sha1].append("Corrupted data")
            if result.user_flags != 0:
                failures[sha1].append("User flag is not zero")
            if result.timestamp < timestamp:
                failures[sha1].append("Bad timestamp")
    assert not failures
