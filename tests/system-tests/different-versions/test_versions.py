import pytest
import socket
import elliptics

from hamcrest import assert_that, has_item, calling, raises

import elliptics_testhelper as et

from logging_tests import logger

@pytest.fixture(scope='module')
def new_nodes(pytestconfig):
    nodes = et.EllipticsTestHelper.get_nodes_from_args(pytestconfig.option.nodes)
    nodes = [n for n in nodes if "server-1" not in n.host and "server-3" not in n.host]
    return nodes

@pytest.fixture(scope='module')
def new_client(new_nodes):
    """Prepares elliptics session w/o nodes with old elliptics
    """
    client = et.EllipticsTestHelper(nodes=new_nodes, wait_timeout=5, check_timeout=30)
    return client

@pytest.fixture(scope='module')
def old_nodes(pytestconfig):
    nodes = et.EllipticsTestHelper.get_nodes_from_args(pytestconfig.option.nodes)
    nodes = [n for n in nodes if "server-1" in n.host or "server-3" in n.host]
    return nodes

@pytest.fixture(scope='module')
def old_client(old_nodes):
    """Prepares elliptics session with nodes with old elliptics
    """
    client = et.EllipticsTestHelper(nodes=old_nodes, wait_timeout=5, check_timeout=30)
    return client

@pytest.mark.new_version
def test_new_nodes(new_client, new_nodes):
    new_ips = map(socket.gethostbyname, [n.host for n in new_nodes])
    for a in new_client.routes.addresses():
        assert_that(new_ips, has_item(a.host))

@pytest.mark.new_version
def test_new_client(old_nodes):
    assert_that(calling(et.EllipticsTestHelper).with_args(nodes=old_nodes,
                                                          wait_timeout=5,
                                                          check_timeout=30),
                raises(elliptics.TimeoutError))

@pytest.mark.old_version
def test_old_nodes(old_client, old_nodes):
    old_ips = map(socket.gethostbyname, [n.host for n in old_nodes])
    for a in old_client.routes.addresses():
        assert_that(old_ips, has_item(a.host))

@pytest.mark.old_version
def test_old_client(new_nodes):
    assert_that(calling(et.EllipticsTestHelper).with_args(nodes=new_nodes,
                                                          wait_timeout=5,
                                                          check_timeout=30),
                raises(elliptics.TimeoutError))
