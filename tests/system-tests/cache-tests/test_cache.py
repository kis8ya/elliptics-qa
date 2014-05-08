import pytest
import sys

import elliptics

import elliptics_testhelper as et

@pytest.fixture(scope='function')
def client():
    """Prepares elliptics.Session object with elliptics.io_flags.cache
    """
    nodes = et.EllipticsTestHelper.get_nodes_from_args(pytest.config.getoption("node"))
    client = et.EllipticsTestHelper(nodes=nodes, wait_timeout=3, check_timeout=30)
    client.set_ioflags(elliptics.io_flags.cache)
    return client

def test_cache_overhead(client):
    """Testing that elliptics will processed commands just in time
    when there is a cache overhead
    """
    count = 100000
    if pytest.config.getoption("show_progress"):
        sys.stdout.write("\n0/{0}".format(count))
        sys.stdout.flush()
    for i in xrange(count):
        key = str(i)
        client.write_data_sync(key, '?')
        if pytest.config.getoption("show_progress"):
            sys.stdout.write('\r{0}/{1}'.format(i + 1, count))
            sys.stdout.flush()
