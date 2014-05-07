import pytest
import socket
import time
import subprocess
import random
import argparse
import math

from collections import defaultdict
from hamcrest import assert_that, raises, calling, equal_to, has_length, contains

import elliptics

import elliptics_testhelper as et

from utils import MB

nodes = et.EllipticsTestHelper.get_nodes_from_args(pytest.config.getoption("node"))
client = et.EllipticsTestHelper(nodes=nodes, wait_timeout=25, check_timeout=30)

class RingPartitioning(object):
    def __init__(self, client):
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

    def is_my_key(self, hostname, elliptics_id):
        r = self.address_ranges[hostname]
        return r[0] <= elliptics_id < r[1]

def drop_nodes(number):
    nodes_to_drop = random.sample(nodes, number)

    for n in nodes_to_drop:
        client.drop_node(n)

    return nodes_to_drop

def drop_groups(group_list):
    groups_to_drop = {}
    for g in group_list:
        groups_to_drop[g] = [n for n in nodes if n.group == g]

    for n in [i for v in groups_to_drop.values() for i in v]:
        client.drop_node(n)

    return groups_to_drop

def wait_routes_list_update():
    wait_time = 30
    wait_count = 6
    print("Waiting for {0} seconds to update client's routes list".format(wait_count * wait_time))
    for i in xrange(wait_count):
        time.sleep(wait_time)
        print("After {0} seconds client's routes list looks like:\n{1}".format(wait_time * (i + 1), client.routes.addresses()))

def write_data_when_dropped(nodes_number):
    """Drops nodes, writes data then resume nodes
    """
    full_ring_partitioning = RingPartitioning(client)

    dropped_nodes = drop_nodes(nodes_number)

    wait_routes_list_update()

    new_ring_partitioning = RingPartitioning(client)

    key_count = pytest.config.getoption("files_number")
    good_keys = defaultdict(list)
    bad_keys = defaultdict(list)
    data_size = 0
    print("Started writing {0} files".format(key_count))
    for i in xrange(key_count):
        key, data = et.key_and_data()
        elliptics_id =  client.transform(key)

        client.write_data_sync(key, data)

        current_host = new_ring_partitioning.get_hostname_by_id(elliptics_id)
        proper_host = full_ring_partitioning.get_hostname_by_id(elliptics_id)
        if current_host == proper_host:
            good_keys[current_host].append(key)
        else:
            bad_keys[current_host].append(key)

        data_size += len(data)
    print("Finished writing files")

    bad_key_count = sum([len(v) for v in bad_keys.values()])
    print("\nDEBUG: with dropped {0} nodes there are {1}/{2} bad keys\ndata size: {3} MB".format(
            [n.host for n in dropped_nodes], bad_key_count, key_count, data_size / MB))

    #TODO: delete this debug output
    print("bad keys")
    for k, v in bad_keys.items():
        print("{0}: {1} bad keys".format(k, len(v)))
        if len(v) >= 3:
            sk = random.sample(v, 3)
        else:
            sk = v
        print(sk)
    print("good keys")
    for k, v in good_keys.items():
        print("{0}: {1} good keys".format(k, len(v)))
        if len(v) >= 3:
            sk = random.sample(v, 3)
        else:
            sk = v
        print(sk)

    for n in dropped_nodes:
        client.resume_node(n)

    wait_routes_list_update()

    return good_keys, bad_keys, dropped_nodes

def find_node(hostname):
    return [n for n in nodes if n.host == hostname][0]

@pytest.fixture(scope='function')
def write_data_when_two_dropped(request):
    def fin():
        client.resume_all_nodes()
        time.sleep(60)
        
    request.addfinalizer(fin)

    return write_data_when_dropped(2)

@pytest.mark.merge
@pytest.mark.timeout(3600)
def test_one_node_option(write_data_when_two_dropped):
    """Tests that 'dnet_recovery --remote ... --one-node merge'
    will recover proper keys from every node
    (even this node was dropped and has no keys to recover)
    when there were 2 dropped nodes
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

    nodes_without_bad_keys = [n for n in nodes if n.host not in bad_keys.keys()]

    # Run dnet_recovery --one-node merge for nodes w/o keys to recover
    for node in nodes_without_bad_keys:
        cmd = ["dnet_recovery",
               "--remote", "{0}:{1}:2".format(node.host, node.port),
               "--groups", ','.join(map(str, client.groups)),
               "--one-node",
               "merge"]

        print(cmd)
        subprocess.call(cmd)

    # check that "bad" keys are not accessible
    for key_list in bad_keys.values():
        for k in key_list:
            assert_that(calling(client.read_data_sync).with_args(k),
                        raises(elliptics.NotFoundError))

    # Run dnet_recovery --one-node merge for nodes with keys to recover
    bad_key_list = [i for v in bad_keys.values() for i in v]
    recovered_keys = []
    for hostname, key_list in bad_keys.items():
        node = find_node(hostname)
        cmd = ["dnet_recovery",
               "--remote", "{0}:{1}:2".format(node.host, node.port),
               "--groups", ','.join(map(str, client.groups)),
               "--one-node",
               "merge"]

        print(cmd)
        subprocess.call(cmd)

        # check that keys are accessible
        print("check that keys are accessible: {0}".format(len(key_list)))
        for k in key_list:
            client.read_data_sync(k)

        recovered_keys.extend(key_list)
        bad_key_list = [i for i in bad_key_list if i not in key_list]

        # check that "bad" keys are not accessible yet
        print("check that bad keys are not accessible yet: {0}".format(len(bad_key_list)))
        for k in bad_key_list:
            assert_that(calling(client.read_data_sync).with_args(k),
                        raises(elliptics.NotFoundError))

    # check accessibility for all keys
    good_key_list = [i for v in good_keys.values() for i in v]
    all_keys = recovered_keys + good_key_list
    print("check accessibility for all keys: {0}".format(len(good_key_list)))
    for k in all_keys:
        data = client.read_data_sync(k).pop().data
        assert_that(k, equal_to(et.utils.get_sha1(data)))

@pytest.mark.merge
@pytest.mark.timeout(3600)
def test_merge_add_two_nodes(write_data_when_two_dropped):
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
    print(cmd)
    subprocess.call(cmd)

    # check all keys and data
    good_keys_list = [k for v in good_keys.values() for k in v]
    bad_keys_list = [k for v in bad_keys.values() for k in v]
    for k in good_keys_list + bad_keys_list:
        data = client.read_data_sync(k).pop().data
        assert_that(k, equal_to(et.utils.get_sha1(data)))

@pytest.fixture(scope='function')
def write_indexes_when_groups_dropped(request):
    """Drops two groups, writes data then resume all nodes from these groups
    """
    indexes = ["index1", "index2", "index3", "index4", "index5"]

    good_keys = defaultdict(list)
    key_count = 127
    print("Started writing {0} files".format(key_count))
    for i in xrange(key_count):
        key, data = et.key_and_data()
        indexes_count = random.randint(1, len(indexes) - 1)
        key_indexes = random.sample(indexes, indexes_count)
        index_data = ["idata_{0}".format(i) for i in key_indexes]

        client.write_data_sync(key, data)
        client.set_indexes(key, key_indexes, index_data).get()
        good_keys[key].extend(key_indexes)
    print("Finished writing files")

    # Drop nodes
    groups_count = len(client.groups)
    groups_count_to_drop = int(math.ceil(groups_count / 2.0))
    groups_to_drop = random.sample(client.groups, groups_count_to_drop)
    dropped_groups = drop_groups(groups_to_drop)

    wait_routes_list_update()

    # Write "bad" keys
    key_count = pytest.config.getoption("files_number")
    bad_keys = defaultdict(list)
    print("Started writing {0} files".format(key_count))
    for i in xrange(key_count):
        key, data = et.key_and_data()
        indexes_count = random.randint(1, len(indexes) - 1)
        key_indexes = random.sample(indexes, indexes_count)
        index_data = ["idata_{0}".format(i) for i in key_indexes]

        client.write_data_sync(key, data)
        client.set_indexes(key, key_indexes, index_data).get()
        bad_keys[key].extend(key_indexes)
    print("Finished writing files")

    for n in [i for v in dropped_groups.values() for i in v]:
        client.resume_node(n)

    wait_routes_list_update()

    def fin():
        client.resume_all_nodes()
        time.sleep(60)
        
    request.addfinalizer(fin)

    return good_keys, bad_keys, dropped_groups, indexes

@pytest.mark.dc
@pytest.mark.timeout(3600)
def test_dc_indexes(write_indexes_when_groups_dropped):
    """Tests that 'dnet_recovery --remote ... dc'
    will recover all keys for every dropped group
    when there were 2 dropped groups
    """
    good_keys, bad_keys, dropped_groups, indexes = write_indexes_when_groups_dropped
    bad_groups = [int(g) for g in dropped_groups.keys()]
    ring_partitioning = RingPartitioning(client)

    print(dropped_groups)
    rkeys = random.sample(bad_keys.keys(), 5)
    for k in rkeys:
        print("{0}: {1}".format(k, bad_keys[k]))

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
               "--groups", ','.join(map(str, client.groups)),
               "dc"]
        print(cmd)
        subprocess.call(cmd)

        for k, v in bad_keys.items():
            if ring_partitioning.is_my_key(node.host, k):
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
        res = client.find_all_indexes([i]).get()
        for r in res:
            for k in good_keys.keys():
                if client.transform(k) == r.id:
                    assert_that(good_keys[k], contains([i]))
                    break
            else:
                raise AssertionError("Found an odd key: elliptics.Id({0}) with unexpected index: {1}".format(r.id, i))
