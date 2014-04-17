import pytest
import socket
import time
import subprocess
import shlex
import sys
import random

from collections import defaultdict

import elliptics

import elliptics_testhelper as et

from utils import MB

class EllipticsTestHelper(et.EllipticsTestHelper):
    DROP_RULE = "INPUT --proto tcp --destination-port {port} --jump DROP"

    @staticmethod
    def drop_node(node):
        """ Makes a node unavailable for elliptics requests
        """
        rule = EllipticsTestHelper.DROP_RULE.format(port=node.port)
        cmd = "ssh -q {host} iptables --append {rule}".format(host=node.host,
                                                              rule=rule)
        subprocess.call(shlex.split(cmd))

    @staticmethod
    def resume_node(node):
        """ Unlocks a node for elliptics requests
        """
        rule = EllipticsTestHelper.DROP_RULE.format(port=node.port)
        cmd = "ssh -q {host} iptables --delete {rule}".format(host=node.host,
                                                              rule=rule)
        subprocess.call(shlex.split(cmd))

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

@pytest.fixture(scope='function')
def write_data():
    full_ring_partitioning = RingPartitioning(client)

    node_to_drop = random.randint(0, len(nodes) - 1)
    client.drop_node(nodes[node_to_drop])

    s = 90
    print("Waiting for {0} seconds to update client's route list".format(s))
    time.sleep(s)

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
    print("\nDEBUG: with droped {0} node there are {0}/{1} bad keys\ndata size: {2} MB".format(
            node_to_drop, bad_key_count, key_count, data_size / MB))

    client.resume_node(nodes[node_to_drop])
    print("\nWaiting for {0} seconds to update client's route list".format(s))
    time.sleep(s)

    return good_keys, bad_keys

@pytest.mark.timeout(3600)
def test_merge(write_data):
    """Testing that execution of 'dnet_recovery merge' command will be successful and
    after merge all keys will belong to proper nodes
    """
    good_keys, bad_keys = write_data

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
