import pytest

import elliptics

import elliptics_testhelper as et
from elliptics_testhelper import nodes

def pytest_addoption(parser):
    parser.addoption('--node', type='string', action='append', dest="nodes")

@pytest.fixture(scope='function')
def client(pytestconfig, nodes):
    """Prepares elliptics.Session object with elliptics.io_flags.cache."""
    client = et.EllipticsTestHelper(nodes=nodes)
    client.set_ioflags(elliptics.io_flags.cache)
    return client
