import pytest

import elliptics

import test_helper.elliptics_testhelper as et
from test_helper.elliptics_testhelper import nodes


@pytest.fixture(scope='function')
def client(pytestconfig, nodes):
    """Prepares elliptics.Session object with elliptics.io_flags.cache."""
    client = et.EllipticsTestHelper(nodes=nodes)
    client.set_ioflags(elliptics.io_flags.cache)
    return client
