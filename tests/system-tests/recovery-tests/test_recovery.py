import pytest
import socket
import time
import subprocess
import random
import argparse
import math

from collections import defaultdict
from hamcrest import assert_that, raises, calling, equal_to

import elliptics

import elliptics_testhelper as et

from elliptics_testhelper import nodes
from utils import MB, get_key_and_data, hasitems
from logging_tests import logger

@pytest.fixture(scope='module')
def client(pytestconfig, nodes):
    """Prepares elliptics session with long timeouts"""
    client = et.EllipticsTestHelper(nodes=nodes, wait_timeout=25, check_timeout=30)
    return client

@pytest.fixture(scope='module')
def files_size(pytestconfig):
    return pytestconfig.option.files_size

@pytest.fixture(scope='module')
def files_number(pytestconfig):
    return pytestconfig.option.files_number

timeout = pytest.config.getoption("test_timeout")

def key_and_data(files_size):
    if files_size:
        return get_key_and_data(files_size, randomize_len=False)
    else:
        return get_key_and_data()

class RingPartitioning(object):
    def __init__(self, client, nodes):
        self.address_ranges = {}
        for node in client.routes.addresses():
            for n in nodes:
                if socket.gethostbyname(n.host) == node.host:
                    hostname = n.host
                    break
            self.address_ranges[hostname] = client.routes.get_address_ranges(node)

    def get_hostname_by_id(self, elliptics_id):
        for hostname, address_ranges in self.address_ranges.items():
            for r in address_ranges:
                if r[0] <= elliptics_id < r[1]:
                    return hostname

    def is_my_id(self, hostname, elliptics_id):
        my = False
        for r in self.address_ranges[hostname]:
            my |= r[0] <= elliptics_id < r[1]
        return my

def drop_nodes(client, nodes, number):
    nodes_to_drop = random.sample(nodes, number)

    for n in nodes_to_drop:
        client.drop_node(n)

    return nodes_to_drop

def drop_groups(client, nodes, group_list):
    groups_to_drop = {}
    for g in group_list:
        groups_to_drop[g] = [n for n in nodes if n.group == g]

    for n in [i for v in groups_to_drop.values() for i in v]:
        client.drop_node(n)

    return groups_to_drop

def wait_routes_list_update(client):
    wait_time = 30
    wait_count = 6
    print("Waiting for {0} seconds to update client's routes list".format(wait_count * wait_time))
    for i in xrange(wait_count):
        time.sleep(wait_time)
        print("After {0} seconds client's routes list looks like:\n{1}".format(wait_time * (i + 1), client.routes.addresses()))

def write_data_when_dropped(client, nodes, files_number, files_size, nodes_number):
    """Drops nodes, writes data then resume nodes
    """
    full_ring_partitioning = RingPartitioning(client, nodes)

    dropped_nodes = drop_nodes(client, nodes, nodes_number)

    wait_routes_list_update(client)

    new_ring_partitioning = RingPartitioning(client, nodes)

    good_keys = defaultdict(list)
    bad_keys = defaultdict(list)
    data_size = 0
    logger.info("Started writing {0} files\n".format(files_number))
    for i in xrange(files_number):
        key, data = key_and_data(files_size)
        elliptics_id =  client.transform(key)

        client.write_data_sync(key, data)

        current_host = new_ring_partitioning.get_hostname_by_id(elliptics_id)
        proper_host = full_ring_partitioning.get_hostname_by_id(elliptics_id)
        if current_host == proper_host:
            good_keys[current_host].append(key)
        else:
            bad_keys[current_host].append(key)

        data_size += len(data)

        logger.info("\r{0}/{1}".format(i + 1, files_number))
    logger.info("\nFinished writing files\n")

    bad_key_count = sum([len(v) for v in bad_keys.values()])
    logger.info("with dropped {0} nodes there are {1}/{2} bad keys\ndata size: {3} MB\n".format(
            [n.host for n in dropped_nodes], bad_key_count, files_number, data_size / MB))

    for n in dropped_nodes:
        client.resume_node(n)

    wait_routes_list_update(client)

    return good_keys, bad_keys, dropped_nodes

def find_node(nodes, hostname):
    return [n for n in nodes if n.host == hostname][0]

@pytest.fixture(scope='function')
def write_data_when_two_dropped(request, client, nodes, files_number, files_size):
    def fin():
        client.resume_all_nodes()
        time.sleep(60)
        
    request.addfinalizer(fin)

    return write_data_when_dropped(client, nodes, files_number, files_size, 2)

@pytest.mark.merge
@pytest.mark.timeout(timeout)
def test_one_node_option(client, nodes, write_data_when_two_dropped):
    """Tests that 'dnet_recovery --remote ... --one-node merge'
    will recover proper keys from every node
    (even this node was dropped and has no keys to recover)
    when there were 2 dropped nodes
    """
    good_keys, bad_keys, dropped_nodes = write_data_when_two_dropped
    ring_partitioning = RingPartitioning(client, nodes)

    # check that "good" keys are accessible
    for key_list in good_keys.values():
        for k in key_list:
            client.read_data_sync(k)
                
    # check that "bad" keys are not accessible
    for key_list in bad_keys.values():
        for k in key_list:
            assert_that(calling(client.read_data_sync).with_args(k),
                        raises(elliptics.NotFoundError))

    # Run dnet_recovery --one-node merge for nodes with keys to recover
    bad_key_list = [i for v in bad_keys.values() for i in v]
    recovered_keys = []
    for node in dropped_nodes:
        cmd = ["dnet_recovery",
               "--remote", "{0}:{1}:2".format(node.host, node.port),
               "--groups", ','.join(map(str, client.groups)),
               "--one-node",
               "merge"]

        logger.info("{0}\n".format(cmd))
        subprocess.call(cmd)

        key_list = []
        for k in bad_key_list:
            elliptics_id = client.transform(k)
            if ring_partitioning.is_my_id(node.host, elliptics_id):
                key_list.append(k)
                bad_key_list.remove(k)

        # check that keys are accessible
        for k in key_list:
            client.read_data_sync(k)

        recovered_keys.extend(key_list)

        # check that "bad" keys are not accessible yet
        for k in bad_key_list:
            assert_that(calling(client.read_data_sync).with_args(k),
                        raises(elliptics.NotFoundError))

    # check accessibility for all keys
    good_key_list = [i for v in good_keys.values() for i in v]
    all_keys = recovered_keys + good_key_list
    for k in all_keys:
        data = client.read_data_sync(k).pop().data
        assert_that(k, equal_to(et.utils.get_sha1(data)))

@pytest.mark.merge
@pytest.mark.timeout(timeout)
def test_merge_add_two_nodes(client, nodes, write_data_when_two_dropped):
    """Tests that 'dnet_recovery --remote ... merge'
    will recover all keys when there were 2 dropped nodes
    """
    good_keys, bad_keys, dropped_nodes = write_data_when_two_dropped

    # check that "good" keys are accessible
    for key_list in good_keys.values():
        for k in key_list:
            client.read_data_sync(k)
                
    # check that "bad" keys are not accessible
    for key_list in bad_keys.values():
        for k in key_list:
            assert_that(calling(client.read_data_sync).with_args(k),
                        raises(elliptics.NotFoundError))

    # Run dnet_recovery merge
    node = random.choice(nodes)
    cmd = ["dnet_recovery",
           "--remote", "{0}:{1}:2".format(node.host, node.port),
           "--groups", ','.join(map(str, client.groups)),
           "merge"]
    logger.info("{0}\n".format(cmd))
    subprocess.call(cmd)

    # check all keys and data
    good_keys_list = [k for v in good_keys.values() for k in v]
    bad_keys_list = [k for v in bad_keys.values() for k in v]
    for k in good_keys_list + bad_keys_list:
        data = client.read_data_sync(k).pop().data
        assert_that(k, equal_to(et.utils.get_sha1(data)))

@pytest.fixture(scope='function')
def write_indexes_when_groups_dropped(request, client, nodes, files_number, files_size):
    """Drops half groups, writes data then resume all nodes from these groups
    """
    # Generate 5 random indexes
    indexes = map(str, random.sample(xrange(10000000), 5))

    good_keys = defaultdict(list)
    key_count = 127
    logger.info("Started writing {0} files\n".format(key_count))
    for i in xrange(key_count):
        key, data = key_and_data(files_size)
        indexes_count = random.randint(1, len(indexes) - 1)
        key_indexes = random.sample(indexes, indexes_count)
        index_data = ["idata_{0}".format(k) for k in key_indexes]

        client.write_data_sync(key, data)
        client.set_indexes(key, key_indexes, index_data).get()
        good_keys[key].extend(key_indexes)

        logger.info("\r{0}/{1}".format(i + 1, key_count))
    logger.info("\nFinished writing files\n")

    # Drop nodes
    groups_count = len(client.groups)
    groups_count_to_drop = int(math.ceil(groups_count / 2.0))
    groups_to_drop = random.sample(client.groups, groups_count_to_drop)
    dropped_groups = drop_groups(client, nodes, groups_to_drop)

    wait_routes_list_update(client)

    # Write "bad" keys
    bad_keys = defaultdict(list)
    logger.info("Started writing {0} files\n".format(files_number))
    for i in xrange(files_number):
        key, data = key_and_data(files_size)
        indexes_count = random.randint(1, len(indexes) - 1)
        key_indexes = random.sample(indexes, indexes_count)
        index_data = ["idata_{0}".format(k) for k in key_indexes]

        client.write_data_sync(key, data)
        client.set_indexes(key, key_indexes, index_data).get()
        bad_keys[key].extend(key_indexes)

        logger.info("\r{0}/{1}".format(i + 1, files_number))
    logger.info("\nFinished writing files\n")

    for n in [i for v in dropped_groups.values() for i in v]:
        client.resume_node(n)

    wait_routes_list_update(client)

    def fin():
        client.resume_all_nodes()
        time.sleep(60)
        
    request.addfinalizer(fin)

    return good_keys, bad_keys, dropped_groups, indexes

@pytest.mark.dc
@pytest.mark.timeout(timeout)
def test_one_node_dc_with_indexes(client, nodes, write_indexes_when_groups_dropped):
    """Tests that 'dnet_recovery --remote ... --one-node dc'
    will recover all keys for every dropped group
    when there were half groups dropped
    """
    good_keys, bad_keys, dropped_groups, indexes = write_indexes_when_groups_dropped
    bad_groups = [int(g) for g in dropped_groups.keys()]
    ring_partitioning = RingPartitioning(client, nodes)

    # check that "good" keys are accessible in all groups
    for g in client.groups:
        for k in good_keys.keys():
            client.read_data_from_groups_sync(k, [g])

    # check that "bad" keys are not accessible in "dropped" groups
    for k in bad_keys.keys():
        assert_that(calling(client.read_data_from_groups_sync).with_args(k, bad_groups),
                    raises(elliptics.NotFoundError))

    # check that recovered keys are accessible in all groups
    good_groups = [g for g in client.groups if g not in bad_groups]
    for g in good_groups:
        for k in bad_keys.keys():
            client.read_data_from_groups_sync(k, [g])

    test_group = random.choice(client.groups)
    test_nodes = [n for n in nodes if n.group == test_group]
    recovered_keys = []
    for node in test_nodes:
        cmd = ["dnet_recovery",
               "--remote", "{0}:{1}:2".format(node.host, node.port),
               "--one-node",
               "--groups", ','.join(map(str, client.groups)),
               "dc"]
        logger.info("{0}\n".format(cmd))
        subprocess.call(cmd)

        for k, v in bad_keys.items():
            elliptics_id = client.transform(k)
            if ring_partitioning.is_my_id(node.host, elliptics_id):
                good_keys[k].extend(v)
                del bad_keys[k]

        # check that bad keys are not accessible yet in "dropped" groups
        for g in dropped_groups.keys():
            for k in bad_keys.keys():
                assert_that(calling(client.read_data_from_groups_sync).with_args(k, [g]),
                            raises(elliptics.NotFoundError))

        # check that "good" keys are accessible in all groups
        for g in client.groups:
            for k in good_keys.keys():
                client.read_data_from_groups_sync(k, [g])

    # check all keys
    for g in client.groups:
        for k in good_keys.keys():
            data = client.read_data_from_groups_sync(k, [g]).pop().data
            assert_that(k, equal_to(et.utils.get_sha1(data)))

    # check indexes
    for i in indexes:
        result = client.find_all_indexes([i]).get()
        result_ids = [r.id for r in result]
        ids_with_index = [client.transform(k) for k, v in good_keys.items() if i in v]
        assert_that(result_ids, hasitems(*ids_with_index))

@pytest.mark.dc
@pytest.mark.timeout(timeout)
def test_dc_with_indexes(client, nodes, write_indexes_when_groups_dropped):
    """Tests that 'dnet_recovery --remote ... dc'
    will recover all keys for every dropped group
    when there were half groups dropped
    """
    good_keys, bad_keys, dropped_groups, indexes = write_indexes_when_groups_dropped
    bad_groups = [int(g) for g in dropped_groups.keys()]
    ring_partitioning = RingPartitioning(client, nodes)

    # check that "good" keys are accessible in all groups
    for g in client.groups:
        for k in good_keys.keys():
            client.read_data_from_groups_sync(k, [g])

    # check that "bad" keys are not accessible in "dropped" groups
    for k in bad_keys.keys():
        assert_that(calling(client.read_data_from_groups_sync).with_args(k, bad_groups),
                    raises(elliptics.NotFoundError))

    # check that recovered keys are accessible in all groups
    good_groups = [g for g in client.groups if g not in bad_groups]
    for g in good_groups:
        for k in bad_keys.keys():
            client.read_data_from_groups_sync(k, [g])

    n = random.randint(0, len(nodes) - 1)
    cmd = ["dnet_recovery",
           "--remote", "{0}:{1}:2".format(nodes[n].host, nodes[n].port),
           "--groups", ','.join(map(str, client.groups)),
           "dc"]
    logger.info("{0}\n".format(cmd))
    subprocess.call(cmd)

    for k, v in bad_keys.items():
        good_keys[k].extend(v)

    # check all keys
    for g in client.groups:
        for k in good_keys.keys():
            data = client.read_data_from_groups_sync(k, [g]).pop().data
            assert_that(k, equal_to(et.utils.get_sha1(data)))

    # check indexes
    for i in indexes:
        result = client.find_all_indexes([i]).get()
        result_ids = [r.id for r in result]
        ids_with_index = [client.transform(k) for k, v in good_keys.items() if i in v]
        assert_that(result_ids, hasitems(*ids_with_index))

@pytest.mark.dc
@pytest.mark.timeout(timeout)
def test_nprocess_dc_with_indexes(client, nodes, write_indexes_when_groups_dropped):
    """Tests that 'dnet_recovery --nprocess=3 --remote ... dc'
    will recover all keys for every dropped group
    when there were half groups dropped
    """
    good_keys, bad_keys, dropped_groups, indexes = write_indexes_when_groups_dropped
    bad_groups = [int(g) for g in dropped_groups.keys()]
    ring_partitioning = RingPartitioning(client, nodes)

    # check that "good" keys are accessible in all groups
    for g in client.groups:
        for k in good_keys.keys():
            client.read_data_from_groups_sync(k, [g])

    # check that "bad" keys are not accessible in "dropped" groups
    for k in bad_keys.keys():
        assert_that(calling(client.read_data_from_groups_sync).with_args(k, bad_groups),
                    raises(elliptics.NotFoundError))

    # check that recovered keys are accessible in all groups
    good_groups = [g for g in client.groups if g not in bad_groups]
    for g in good_groups:
        for k in bad_keys.keys():
            client.read_data_from_groups_sync(k, [g])

    n = random.randint(0, len(nodes) - 1)
    cmd = ["dnet_recovery",
           "--nprocess", "3",
           "--remote", "{0}:{1}:2".format(nodes[n].host, nodes[n].port),
           "--groups", ','.join(map(str, client.groups)),
           "dc"]
    logger.info("{0}\n".format(cmd))
    subprocess.call(cmd)

    for k, v in bad_keys.items():
        good_keys[k].extend(v)

    # check all keys
    for g in client.groups:
        for k in good_keys.keys():
            data = client.read_data_from_groups_sync(k, [g]).pop().data
            assert_that(k, equal_to(et.utils.get_sha1(data)))

    # check indexes
    for i in indexes:
        result = client.find_all_indexes([i]).get()
        result_ids = [r.id for r in result]
        ids_with_index = [client.transform(k) for k, v in good_keys.items() if i in v]
        assert_that(result_ids, hasitems(*ids_with_index))
