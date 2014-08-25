import pytest
import socket
import elliptics
import errno
import os

from hamcrest import assert_that, has_item, calling, raises

import test_helper.elliptics_testhelper as et
from test_helper.matchers import raises_elliptics_error

def is_odd_node(node):
    """Checks if node has odd number"""
    node_number = int(node.host.partition('.')[0].rsplit("-", 1)[-1])
    return node_number % 2

@pytest.fixture(scope='module')
def new_nodes(pytestconfig):
    """Returns list of nodes with new elliptics version (v2.26)."""
    nodes = et.EllipticsTestHelper.get_nodes_from_args(pytestconfig.option.nodes)
    nodes = [n for n in nodes if not is_odd_node(n)]
    return nodes

@pytest.fixture(scope='function')
def new_client_node():
    """Returns client node for elliptics v2.26."""
    log_path = "/var/log/elliptics/new_client.log"
    dir_path = os.path.dirname(log_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    elog = elliptics.Logger(log_path, elliptics.log_level.debug)

    config = elliptics.Config()

    client_node = elliptics.Node(elog, config)

    return client_node

@pytest.fixture(scope='function')
def new_session(new_client_node, new_nodes):
    """Returns elliptics session with nodes with new elliptics version (v2.26)."""
    addresses = [elliptics.Address(node.host, node.port, socket.AF_INET)
                 for node in new_nodes]
    new_client_node.add_remotes(addresses)

    elliptics_session = elliptics.Session(new_client_node)

    # Get uniq groups from nodes list
    groups = list({node.group for node in new_nodes})
    elliptics_session.groups = groups

    return elliptics_session

@pytest.fixture(scope='module')
def old_nodes(pytestconfig):
    """Returns list of nodes with old elliptics version (v2.25)."""
    nodes = et.EllipticsTestHelper.get_nodes_from_args(pytestconfig.option.nodes)
    nodes = [n for n in nodes if is_odd_node(n)]
    return nodes

@pytest.fixture(scope='function')
def old_client_node():
    """Returns client node for elliptics v2.25."""
    log_path = "/var/log/elliptics/old_client.log"
    dir_path = os.path.dirname(log_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    elog = elliptics.Logger(log_path, 4)

    config = elliptics.Config()

    client_node = elliptics.Node(elog, config)

    return client_node

@pytest.fixture(scope='function')
def old_session(old_client_node, old_nodes):
    """Prepares elliptics session with nodes with old elliptics version (v2.25)."""
    addresses = [elliptics.Address(node.host, node.port, socket.AF_INET)
                 for node in old_nodes]
    old_client_node.add_remote(addresses)

    elliptics_session = elliptics.Session(old_client_node)

    # Get uniq groups from nodes list
    groups = list({node.group for node in old_nodes})
    elliptics_session.groups = groups

    return elliptics_session

@pytest.mark.new_version
def test_new_nodes(new_session, new_nodes):
    """Testing that nodes (with new elliptics version)
    will not add nodes with old elliptics version to elliptics client
    """
    new_ips = map(socket.gethostbyname, [node.host for node in new_nodes])
    for address in new_session.routes.addresses():
        assert_that(new_ips, has_item(address.host))

@pytest.mark.new_version
def test_new_client(new_client_node, old_nodes):
    """Testing that client (with new elliptics version)
    will not add nodes with old elliptics version
    """
    addresses = [elliptics.Address(node.host, node.port, socket.AF_INET)
                 for node in old_nodes]
    assert_that(calling(new_client_node.add_remotes).with_args(addresses),
                raises(elliptics.Error),
                "New client didn't raise expected exception")

@pytest.mark.old_version
def test_old_nodes(old_session, old_nodes):
    """Testing that nodes (with old elliptics version)
    will not add nodes with new elliptics version to elliptics client
    """
    old_ips = map(socket.gethostbyname, [node.host for node in old_nodes])
    for address in old_session.routes.addresses():
        assert_that(old_ips, has_item(address.host))

@pytest.mark.old_version
def test_old_client(old_client_node, new_nodes):
    """Testing that client (with new elliptics version)
    will not add nodes with old elliptics version
    """
    for node in new_nodes:
        assert_that(calling(old_client_node.add_remote).with_args(node.host,
                                                                  node.port,
                                                                  socket.AF_INET),
                    raises(elliptics.Error),
                    "New client didn't raise expected exception")
