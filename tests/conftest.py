import pytest
import os.path
import elliptics
import socket

from test_helper.elliptics_testhelper import nodes
from test_helper import utils


def pytest_addoption(parser):
    parser.addoption('--node', type='string', action='append', dest="nodes",
                     help="Elliptics node. Example: --node=hostname:port:group")
    parser.addoption('--wait-timeout', type=int, help="Elliptics wait_timeout.")
    parser.addoption('--check-timeout', type=int, help="Elliptics check_timeout.")


@pytest.fixture(scope='module')
def nodes(request):
    """Returns list of nodes."""
    return utils.get_nodes_from_option(request.config.option.nodes)


@pytest.fixture(scope='function')
def session(request, nodes):
    """Returns prepared elliptics.Session."""
    return utils.create_session(nodes,
                                request.config.option.check_timeout,
                                request.config.option.wait_timeout,
                                request.node.name)
