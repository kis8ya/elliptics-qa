import pytest
import os.path
import elliptics
import socket

from test_helper.elliptics_testhelper import nodes


def pytest_addoption(parser):
    parser.addoption('--node', type='string', action='append', dest="nodes",
                     help="Elliptics node. Example: --node=hostname:port:group")
    parser.addoption('--wait-timeout', type='int', dest="wait_timeout",
                     help="Elliptics wait_timeout.")
    parser.addoption('--check-timeout', type='int', dest="check_timeout",
                     help="Elliptics check_timeout.")


@pytest.fixture(scope='module')
def session(pytestconfig, nodes):
    """Returns prepared elliptics.Session."""
    log_path = "/var/log/elliptics/client.log"
    dir_path = os.path.dirname(log_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    elog = elliptics.Logger(log_path, elliptics.log_level.debug)

    config = elliptics.Config()
    if pytestconfig.option.wait_timeout:
        config.wait_timeout = pytestconfig.option.wait_timeout
    if pytestconfig.option.check_timeout:
        config.check_timeout = pytestconfig.option.check_timeout

    client_node = elliptics.Node(elog, config)
    addresses = [elliptics.Address(node.host, node.port, socket.AF_INET)
                 for node in nodes]
    client_node.add_remotes(addresses)

    elliptics_session = elliptics.Session(client_node)

    # Get uniq groups from nodes list
    groups = list({n.group for n in nodes})
    elliptics_session.groups = groups

    return elliptics_session
