# -*- coding: utf8 -*-

import elliptics
import hashlib
import random
import pytest
import os
from collections import defaultdict

MIN_SIZE = 100
MAX_SIZE = 1000
config = pytest.config
BATCH_SIZE = config.getoption('batch_size') 
BATCH_NUMBER = config.getoption('batch_number') 
WRITE_TIMEOUT = config.getoption('write_timeout') 
WAIT_TIMEOUT = config.getoption('wait_timeout') 
HOSTS = config.getoption('host')
keys = set()

def create_random_file(size):
    data = os.urandom(size)
    key = get_sha1(data)
    return key, data

def get_sha1(data):
    m = hashlib.sha1()
    m.update(data)
    return m.hexdigest()

# Настройка сессии с Elliptics
elog = elliptics.Logger("/dev/stderr", 0)
node = elliptics.Node(elog)
node.set_timeouts(WRITE_TIMEOUT, WAIT_TIMEOUT)
for host in HOSTS:
    node.add_remote(host, 1025)
s = elliptics.Session(node)
s.groups = [1]
ids_batches = []
timestamp = elliptics.Time.now()

@pytest.fixture()
def put_keys(request):
    for i in xrange(BATCH_NUMBER):
        ids = []
        results = []
        # Генерация и асинхронная запись пачки файлов
        for j in xrange(BATCH_SIZE):
            size = random.randint(MIN_SIZE, MAX_SIZE)
            key, data = create_random_file(size)
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
