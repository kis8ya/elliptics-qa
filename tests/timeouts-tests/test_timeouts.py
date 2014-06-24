# -*- coding: utf-8 -*-
#
import pytest
import elliptics
import time
import random
import math

from hamcrest import assert_that, calling, raises, less_than, greater_than, all_of

import elliptics_testhelper as et
import utils

from elliptics_testhelper import key_and_data, nodes
from utils import MB

@pytest.fixture(scope='function')
def client(pytestconfig, nodes):
    """Prepares elliptics session with custom timeouts."""
    wait_timeout = pytestconfig.option.wait_timeout
    check_timeout = pytestconfig.option.check_timeout
    client = et.EllipticsTestHelper(nodes,
                                    wait_timeout=wait_timeout,
                                    check_timeout=check_timeout)
    return client

@pytest.fixture(scope='function')
def write_and_drop_node(request, client, key_and_data):
    """ Writes data and drops a node (the one, at which data was written)
    """
    key, data = key_and_data
    result = client.write_data_sync(key, data).pop()
    node = result.storage_address
    client.drop_node(node)

    def teardown():
        client.resume_node(node)

    request.addfinalizer(teardown)
    return client, key

@pytest.mark.groups_1
def test_wait_timeout(write_and_drop_node):
    """ Testing that reading data (which stores at unavailable node)
    will raise the exception
    """
    client, key = write_and_drop_node

    # Additional 3 seconds for functions calls and networking stuff
    DELAY = 3
    start_time = time.time()
    assert_that(calling(client.read_data_sync).with_args(key),
                raises(elliptics.TimeoutError, et.EllipticsTestHelper.error_info.TimeoutError))
    exec_time = time.time() - start_time

    wait_timeout = client.get_timeout()
    assert_that(all_of(exec_time, greater_than(wait_timeout),
                       exec_time, less_than(wait_timeout + DELAY)))

@pytest.fixture(scope='function')
def write_with_quorum_check(request, client, key_and_data):
    """ Sets checker (to checking by quorum) for elliptics session
    and starts data writing
    """
    # Data size depends on WAIT_TIMEOUT and networking limitations
    # (see elliptics_testhelper.set_networking_limitations()).
    # Change this value depend on your network connection.
    size = 6*MB

    data = utils.get_data(size=size, randomize_len=False)
    key = utils.get_sha1(data)

    client.set_checker(elliptics.checkers.quorum)

    et.set_networking_limitations()
    res = client.write_data(key, data)

    def teardown():
        et.clear_networking_limitations()

    request.addfinalizer(teardown)
    
    return (client, res)

@pytest.fixture(scope='function')
def quorum_checker_positive(request, nodes, write_with_quorum_check):
    """ Chooses random nodes and drops them
    (nodes_count = ceil(groups_count / 2) - 1)
    """
    client, res = write_with_quorum_check
    dnodes_count = len(nodes) >> 1 - (len(nodes) & 1 ^ 1)
    dnodes = random.sample(nodes, dnodes_count)

    for node in dnodes:
        client.drop_node(node)

    def teardown():
        for node in dnodes:
            client.resume_node(node)

    request.addfinalizer(teardown)

    return res

@pytest.mark.groups_3
def test_quorum_checker_positive(quorum_checker_positive):
    """ Testing that writing will be finished successfully
    when less than half of elliptics groups are unavailable
    """
    async_result = quorum_checker_positive

    async_result.get()

@pytest.fixture(scope='function')
def quorum_checker_negative(request, nodes, write_with_quorum_check):
    """ Chooses random nodes and drops it
    (nodes_count = ceil(groups_count / 2))
    """
    client, res = write_with_quorum_check
    dnodes_count = int(math.ceil(len(nodes) / 2.0))
    dnodes = random.sample(nodes, dnodes_count)
    
    for node in dnodes:
        client.drop_node(node)

    def teardown():
        for node in dnodes:
            client.resume_node(node)

    request.addfinalizer(teardown)

    return res

@pytest.mark.groups_3
def test_quorum_checker_negative(quorum_checker_negative):
    """ Testing that writing will raise the exception
    when half or more elliptics groups are unavailable
    """
    async_result = quorum_checker_negative

    assert_that(calling(async_result.get),
                raises(elliptics.Error, et.EllipticsTestHelper.error_info.AddrNotExists))

@pytest.fixture(scope='function')
def client_shuffling_off(pytestconfig, nodes):
    """Prepares elliptics session with cleared groups shuffling flag"""
    config = elliptics.Config()
    config.flags &= ~elliptics.config_flags.mix_stats

    wait_timeout = pytestconfig.option.wait_timeout
    check_timeout = pytestconfig.option.check_timeout

    client = et.EllipticsTestHelper(nodes,
                                    wait_timeout=wait_timeout,
                                    check_timeout=check_timeout,
                                    config=config)

    return client

@pytest.fixture(scope='function')
def write_and_shuffling_off(request, client_shuffling_off, nodes, key_and_data):
    """ Turns off groups shuffling, writes data (in all groups),
    chooses two random groups and drops a node from the first random group
    """
    key, data = key_and_data
    client = client_shuffling_off

    client.write_data_sync(key, data)

    groups = random.sample(client.get_groups(), 2)
    node = filter(lambda n: n.group == groups[0], nodes)[0]

    client.drop_node(node)
    
    def teardown():
        client.resume_node(node)

    request.addfinalizer(teardown)

    return (client, key, groups)

@pytest.mark.groups_3
def test_read_from_groups(write_and_shuffling_off):
    """ Testing that reading from two groups will takes T seconds
    when the first group is unavailable
    (where WAIT_TIMEOUT < T < 2 * WAIT_TIMEOUT)
    """
    client, key, groups = write_and_shuffling_off
    
    start_time = time.time()
    client.read_data_from_groups(key, groups).get()
    exec_time = time.time() - start_time

    wait_timeout = client.get_timeout()
    assert_that(all_of(exec_time, greater_than(wait_timeout),
                       exec_time, less_than(wait_timeout * 2)))

#TODO: move this function to lib/utils.py
def drop_groups(client, nodes, group_list):
    groups_to_drop = {}
    for g in group_list:
        groups_to_drop[g] = [n for n in nodes if n.group == g]

    for n in [i for v in groups_to_drop.values() for i in v]:
        client.drop_node(n)

    return groups_to_drop

@pytest.fixture(scope='function')
def prepare_keepalive_check(request, client, nodes, key_and_data):
    """Writes file and drops nodes from half groups
    """
    key, data = key_and_data

    client.write_data_sync(key, data)

    # drop half groups
    groups_count = len(client.groups)
    groups_count_to_drop = int(math.ceil(groups_count / 2.0))
    groups_to_drop = random.sample(client.groups, groups_count_to_drop)
    dropped_groups = drop_groups(client, nodes, groups_to_drop)

    available_groups = {}
    ag = [g for g in client.groups if g not in dropped_groups.keys()]
    for g in ag:
        available_groups[g] = [n for n in nodes if n.group == g]

    def fin():
        client.resume_all_nodes()
        time.sleep(60)

    request.addfinalizer(fin)

    return client, key, dropped_groups, available_groups

@pytest.mark.keepalive
def test_keepalive(prepare_keepalive_check):
    """Testing that reading files from unavailable groups will raise the
    elliptics.Error("...No such device or address: -6") after certain time of downtime
    """
    client, key, unavailable_groups, available_groups = prepare_keepalive_check

    for g in available_groups.keys():
        client.read_data_from_groups_sync(key, [g])

    for g in unavailable_groups.keys():
        assert_that(calling(client.read_data_from_groups_sync).with_args(key, [g]),
                    raises(elliptics.TimeoutError))

    wait_time = 180
    print("Waiting for {0} seconds to update client's routes list".format(wait_time))
    time.sleep(wait_time)

    for g in available_groups.keys():
        client.read_data_from_groups_sync(key, [g])

    for g in unavailable_groups.keys():
        assert_that(calling(client.read_data_from_groups_sync).with_args(key, [g]),
                    raises(elliptics.Error, client.error_info.AddrNotExists))

    client.resume_all_nodes()
    time.sleep(60)

    for g in client.groups:
        client.read_data_from_groups_sync(key, [g])
