# -*- coding: utf-8 -*-

import elliptics
import hashlib
import random
import pytest
import os

from collections import defaultdict

import elliptics_testhelper as et

from elliptics_testhelper import nodes
from utils import get_key_and_data, get_sha1

@pytest.fixture(scope='module')
def client(pytestconfig, nodes):
    """Prepares elliptics session with custom timeouts."""
    check_timeout = pytestconfig.option.check_timeout
    wait_timeout = pytestconfig.option.wait_timeout
    client = et.EllipticsTestHelper(nodes=nodes, wait_timeout=25, check_timeout=30)
    return client

@pytest.fixture(scope='module')
def timestamp():
    timestamp = elliptics.Time.now()
    return timestamp

@pytest.fixture()
def ids_batches(request, pytestconfig, client, timestamp):
    MIN_SIZE = 100
    MAX_SIZE = 1000
    batch_size = pytestconfig.option.batch_size
    batch_number = pytestconfig.option.batch_number
    ids_batches = []
    for i in xrange(batch_number):
        ids = []
        results = []
        # Generate and asynchronous writing a bunch of data
        for j in xrange(batch_size):
            size = random.randint(MIN_SIZE, MAX_SIZE)
            key, data = get_key_and_data(size, randomize_len=False)
            elliptics_id = client.transform(key)
            ids.append(elliptics_id)
            result = client.write_data(elliptics_id, data)
            results.append(result)
        ids_batches.append(ids)
        for result in results:
            result.get()
    return ids_batches

def test_elliptics(client, ids_batches, timestamp):
    failures = defaultdict(list)
    for ids in ids_batches:
        for result in client.bulk_read(ids):
            data = str(result.data)
            elliptics_id = result.id
            sha1 = get_sha1(data)
            actual_elliptics_id = client.transform(sha1)
            if elliptics_id != actual_elliptics_id:
                failures[sha1].append("Corrupted data")
            if result.user_flags != 0:
                failures[sha1].append("User flag is not zero")
            if result.timestamp < timestamp:
                failures[sha1].append("Bad timestamp")
    assert not failures
