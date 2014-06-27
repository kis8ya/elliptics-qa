import pytest
import time
import socket

import elliptics_testhelper as et

from utils import get_key_and_data

def key_and_data(files_size):
    """Returns key-data pair."""
    if files_size:
        return get_key_and_data(files_size, randomize_len=False)
    else:
        return get_key_and_data()

class RingPartitioning(object):
    """This class helps to store information about ring partitioning  for elliptics nodes."""
    def __init__(self, client, nodes):
        self.address_ranges = {}
        for node in client.routes.addresses():
            for n in nodes:
                if socket.gethostbyname(n.host) == node.host:
                    hostname = n.host
                    break
            self.address_ranges[hostname] = client.routes.get_address_ranges(node)

    def get_hostname_by_id(self, elliptics_id):
        """Returns hostname of elliptics node which should owns given elliptics ID."""
        for hostname, address_ranges in self.address_ranges.items():
            for r in address_ranges:
                if r[0] <= elliptics_id < r[1]:
                    return hostname

    def is_my_id(self, hostname, elliptics_id):
        """Checks does given elliptics node owns given elliptics ID."""
        my = False
        for r in self.address_ranges[hostname]:
            my |= r[0] <= elliptics_id < r[1]
        return my

def wait_routes_list_update(client):
    """Waits 180 seconds to update client's routes list."""
    wait_time = 30
    wait_count = 6
    print("Waiting for {0} seconds to update client's routes list".format(wait_count * wait_time))
    for i in xrange(wait_count):
        time.sleep(wait_time)
        print("After {0} seconds client's routes list looks like:\n{1}".format(wait_time * (i + 1), client.routes.addresses()))

@pytest.fixture(scope='module')
def client(pytestconfig, nodes):
    """Prepares elliptics session with long timeouts"""
    client = et.EllipticsTestHelper(nodes=nodes, wait_timeout=25, check_timeout=30)
    return client

@pytest.fixture(scope='module')
def files_size(pytestconfig):
    """Returns files size from test options."""
    return pytestconfig.option.files_size

@pytest.fixture(scope='module')
def files_number(pytestconfig):
    """Returns numbers of files from test options."""
    return pytestconfig.option.files_number

def pytest_addoption(parser):
    parser.addoption('--files_number', type='int', default='127',
                     help="Amount of files to write.")
    parser.addoption('--files_size', type='int', default=0,
                     help="Amount of bytes for a single file.")
    parser.addoption('--node', type='string', action='append', dest="nodes",
                     help="Elliptics node. Example: --node=hostname:port:group")
