import pytest
import socket
import elliptics

from hamcrest import assert_that, has_item, calling, raises

import elliptics_testhelper as et

from logging_tests import logger

def is_odd_node(node):
    """Checks if node has odd number"""
    node_number = int(node.host.partition('.')[0].rsplit("-", 1)[-1])
    return node_number % 2

@pytest.fixture(scope='module')
def new_nodes(pytestconfig):
    """Returns list of nodes with new elliptics version (2.25)."""
    nodes = et.EllipticsTestHelper.get_nodes_from_args(pytestconfig.option.nodes)
    nodes = [n for n in nodes if not is_odd_node(n)]
    return nodes

@pytest.fixture(scope='module')
def new_client(new_nodes):
    """Prepares elliptics session with nodes with new elliptics version (2.25)."""
    client = et.EllipticsTestHelper(nodes=new_nodes, wait_timeout=5, check_timeout=30)
    return client

@pytest.fixture(scope='module')
def old_nodes(pytestconfig):
    """Returns list of nodes with old elliptics version (2.24)."""
    nodes = et.EllipticsTestHelper.get_nodes_from_args(pytestconfig.option.nodes)
    nodes = [n for n in nodes if is_odd_node(n)]
    return nodes

@pytest.fixture(scope='module')
def old_client(old_nodes):
    """Prepares elliptics session with nodes with old elliptics version (2.24)."""
    client = et.EllipticsTestHelper(nodes=old_nodes, wait_timeout=5, check_timeout=30)
    return client

@pytest.mark.new_version
def test_new_nodes(new_client, new_nodes):
    """Testing that nodes (with new elliptics version)
    will not add nodes with old elliptics version to elliptics client
    """
    new_ips = map(socket.gethostbyname, [n.host for n in new_nodes])
    for a in new_client.routes.addresses():
        assert_that(new_ips, has_item(a.host))

@pytest.mark.new_version
def test_new_client(old_nodes):
    """Testing that client (with new elliptics version)
    will not add nodes with old elliptics version
    """
    assert_that(calling(et.EllipticsTestHelper).with_args(nodes=old_nodes,
                                                          wait_timeout=5,
                                                          check_timeout=30),
                raises(elliptics.TimeoutError))

@pytest.mark.old_version
def test_old_nodes(old_client, old_nodes):
    """Testing that nodes (with old elliptics version)
    will not add nodes with new elliptics version to elliptics client
    """
    old_ips = map(socket.gethostbyname, [n.host for n in old_nodes])
    for a in old_client.routes.addresses():
        assert_that(old_ips, has_item(a.host))

@pytest.mark.old_version
def test_old_client(new_nodes):
    """Testing that client (with new elliptics version)
    will not add nodes with old elliptics version
    """
    assert_that(calling(et.EllipticsTestHelper).with_args(nodes=new_nodes,
                                                          wait_timeout=5,
                                                          check_timeout=30),
                raises(elliptics.TimeoutError))
