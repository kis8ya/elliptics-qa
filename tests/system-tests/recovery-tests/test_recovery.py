import pytest
import socket
import time
import subprocess
import shlex
import sys
import random
import argparse

from collections import defaultdict
from hamcrest import assert_that, raises, calling, equal_to

import elliptics

import elliptics_testhelper as et

from utils import MB

class EllipticsTestHelper(et.EllipticsTestHelper):
    def read_data_from_groups_sync(self, key, groups, offset=0):
        return self.read_data_from_groups(key, groups=groups, offset=offset).get()
    
    DROP_RULES = ["INPUT --proto tcp --destination-port {port} --jump DROP",
                  "OUTPUT --proto tcp --destination-port {port} --jump DROP",
                  "OUTPUT --proto tcp --source-port {port} --jump DROP"]

    def drop_node(self, node):
        """ Makes a node unavailable for elliptics requests
        """
        for drop_rule in EllipticsTestHelper.DROP_RULES:
            rule = drop_rule.format(port=node.port)
            cmd = "ssh -q {host} iptables --append {rule}".format(host=node.host,
                                                                  rule=rule)
            subprocess.call(shlex.split(cmd))
            print(cmd)
            self.dropped_nodes.append(node)

    def resume_node(self, node):
        """ Unlocks a node for elliptics requests
        """
        for drop_rule in EllipticsTestHelper.DROP_RULES:
            rule = drop_rule.format(port=node.port)
            cmd = "ssh -q {host} iptables --delete {rule}".format(host=node.host,
                                                                  rule=rule)
            subprocess.call(shlex.split(cmd))
            self.dropped_nodes.remove(node)

    def resume_all_nodes(self):
        """ Unlocks all nodes for elliptics requests
        """
        for node in self.dropped_nodes:
            self.resume_node(node)

nodes = EllipticsTestHelper.get_nodes_from_args(pytest.config.getoption("node"))
client = EllipticsTestHelper(nodes=nodes, wait_timeout=25, check_timeout=30)

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

def write_data_when_dropped(nodes_number):
    full_ring_partitioning = RingPartitioning(client)

    dropped_nodes = drop_nodes(nodes_number)

    wait_time = 30
    wait_count = 6
    print("Waiting for {0} seconds to update client's route list".format(wait_count * wait_time))
    for i in xrange(wait_count):
        time.sleep(wait_time)
        print("After {0} seconds client's routes list looks like:\n{1}".format(wait_time * (i + 1), client.routes.addresses()))

    new_ring_partitioning = RingPartitioning(client)

    key_count = pytest.config.getoption("files_number")
    good_keys = defaultdict(list)
    bad_keys = defaultdict(list)
    data_size = 0
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
        sys.stdout.write("\r{0}/{1}".format(i + 1, key_count))
        sys.stdout.flush()

    bad_key_count = sum([len(v) for v in bad_keys.values()])
    print("\nDEBUG: with dropped {0} nodes there are {1}/{2} bad keys\ndata size: {3} MB".format(
            [n.host for n in dropped_nodes], bad_key_count, key_count, data_size / MB))

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
    print("\nWaiting for {0} seconds to update client's route list".format(wait_count * wait_time))
    time.sleep(wait_count * wait_time)

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

@pytest.mark.four_servers
@pytest.mark.timeout(3600)
def test_one_node_option(write_data_when_two_dropped):
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

    normal_nodes = [n for n in nodes if n.host not in bad_keys.keys()]

    for node in normal_nodes:
        cmd = ["dnet_recovery",
               "--remote", "{0}:{1}:2".format(node.host, node.port),
               "--one-node",
               "merge"]

        print(cmd)
        subprocess.call(cmd)

    # check that "bad" keys are not accessible
    for key_list in bad_keys.values():
        for k in key_list:
            assert_that(calling(client.read_data_sync).with_args(k),
                        raises(elliptics.NotFoundError))

    bad_key_list = [i for v in bad_keys.values() for i in v]
    recovered_keys = []
    for hostname, key_list in bad_keys.items():
        node = find_node(hostname)
        cmd = ["dnet_recovery",
               "--remote", "{0}:{1}:2".format(node.host, node.port),
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

@pytest.mark.four_servers
@pytest.mark.timeout(3600)
def test_merge_add_two_nodes(write_data_when_two_dropped):
    good_keys, bad_keys, dropped_nodes = write_data_when_two_dropped

    # dnet_recovery merge
    cmd = "dnet_recovery --remote={0}:{1}:2 merge".format(nodes[0].host, nodes[0].port)
    print(cmd)
    output = subprocess.check_output(shlex.split(cmd))
    print(output)

    # check keys and data
    good_keys_list = [k for v in good_keys.values() for k in v]
    bad_keys_list = [k for v in bad_keys.values() for k in v]
    print("DEBUG: {0}".format(len(good_keys_list + bad_keys_list)))
    bad_key_count = 0
    for k in good_keys_list + bad_keys_list:
        try:
            data = client.read_data_sync(k).pop().data

            if et.utils.get_sha1(data) != k:
                bad_key_count += 1
        except elliptics.NotFoundError:
            bad_key_count += 1

    assert not bad_key_count, "There are {0} bad keys after merge (there were {1} before merge)".format(bad_key_count, len(bad_keys))

@pytest.fixture(scope='function')
def write_when_groups_dropped():
    groups_to_drop = random.sample(client.groups, 2)
    dropped_groups = drop_groups(groups_to_drop)

    wait_time = 30
    wait_count = 5
    print("Waiting for {0} seconds to update client's route list".format(wait_count * wait_time))
    for i in xrange(wait_count):
        time.sleep(wait_time)
        client.routes.addresses()

    key_count = pytest.config.getoption("files_number")
    key_list = []
    for i in xrange(key_count):
        key, data = et.key_and_data()

        client.write_data_sync(key, data)
        key_list.append(key)

        sys.stdout.write("\r{0}/{1}".format(i + 1, key_count))
        sys.stdout.flush()

    for n in [i for v in dropped_groups.values() for i in v]:
        client.resume_node(n)
    print("\nWaiting for {0} seconds to update client's route list".format(wait_count * wait_time))
    time.sleep(wait_count * wait_time)

    return key_list, dropped_groups

@pytest.mark.dc
@pytest.mark.timeout(3600)
def test_dc(write_when_groups_dropped):
    key_list, dropped_groups = write_when_groups_dropped
    bad_groups = [int(g) for g in dropped_groups.keys()]

    print(dropped_groups)
    print(random.sample(key_list, 5))

    # check that keys are not accessible in "dropped" groups
    for k in key_list:
        assert_that(calling(client.read_data_from_groups_sync).with_args(k, bad_groups),
                    raises(elliptics.NotFoundError))

    recovered_groups = []
    for g, node_list in dropped_groups.items():
        group = int(g)
        for node in node_list:
            cmd = ["dnet_recovery",
                   "--remote", "{0}:{1}:2".format(node.host, node.port),
                   "dc"]
            print(cmd)
            subprocess.call(cmd)

        bad_groups.remove(group)
        recovered_groups.append(group)

        if bad_groups:
            # check that keys are not accessible in "dropped" groups
            for k in key_list:
                assert_that(calling(client.read_data_from_groups_sync).with_args(k, bad_groups),
                            raises(elliptics.NotFoundError))

        # check that keys are accessible in "recovered" groups
        for k in key_list:
            client.read_data_from_groups_sync(k, recovered_groups)

    # check all keys
    for k in key_list:
        for g in client.groups:
            data = client.read_data_from_groups_sync(k, [g]).pop().data
            assert_that(k, equal_to(et.utils.get_sha1(data)))
