import pytest
import socket
import time
import subprocess
import shlex
import sys

import elliptics

import elliptics_testhelper as et

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
                if elliptics_id >= r[0] and \
                   elliptics_id < r[1]:
                    return hostname

nodes = et.EllipticsTestHelper.get_nodes_from_args(pytest.config.getoption("node"))
client = EllipticsTestHelper(nodes=nodes, wait_timeout=25, check_timeout=30)

@pytest.fixture(scope='module')
def write_data():
    full_ring_partitioning = RingPartitioning(client)

    node_to_drop = 1
    client.drop_node(nodes[node_to_drop])

    s = 90
    print("Waiting for {0} seconds to update client's route list".format(s))
    time.sleep(s)

    new_ring_partitioning = RingPartitioning(client)

    key_count = pytest.config.getoption("files_number")
    keys = []
    bad_keys = []
    data_size = 0
    for i in xrange(key_count):
        key, data = et.key_and_data()
        keys.append(key)
        elliptics_id =  client.transform(key)

        client.write_data_sync(key, data)

        if new_ring_partitioning.get_hostname_by_id(elliptics_id) != \
           full_ring_partitioning.get_hostname_by_id(elliptics_id):
            bad_keys.append(key)

        data_size += len(data)
        sys.stdout.write("\r{0}/{1}".format(i + 1, key_count))
        sys.stdout.flush()

    print("\nDONE: {0}/{1} bad keys\ndata size: {2} MB".format(len(bad_keys), key_count, data_size / (1 << 20)))

    client.resume_node(nodes[node_to_drop])
    print("\nWaiting for {0} seconds to update client's route list".format(s))
    time.sleep(s)

    return keys, bad_keys

@pytest.mark.timeout(3600)
def test_merge(write_data):
    """Testing that execution of 'dnet_recovery merge' command will be successful and
    after merge all keys will belong to proper nodes
    """
    keys, bad_keys = write_data

    # dnet_recovery merge
    cmd = "dnet_recovery --remote={0}:{1}:2 merge".format(nodes[0].host, nodes[0].port)
    print(cmd)
    output = subprocess.check_output(shlex.split(cmd))
    print(output)

    # check keys and data
    bad_key_count = 0
    for k in keys:
        try:
            data = client.read_data_sync(k).pop().data

            if et.utils.get_sha1(data) != k:
                bad_key_count += 1
        except elliptics.NotFoundError:
            bad_key_count += 1

    assert not bad_key_count, "There are {0} bad keys after merge (there were {1} before merge)".format(bad_key_count, len(bad_keys))
