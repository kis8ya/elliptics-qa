import pytest
import elliptics
import socket
import json

from hamcrest import assert_that, all_of, greater_than_or_equal_to, less_than_or_equal_to

from test_helper import utils, network
from test_helper.elliptics_testhelper import nodes
from test_helper.logging_tests import logger


LOG_FILE = "elliptics_client.log"


DATA_LENGTH = 1


LOW_DELAY = 0
HIGH_DELAY = 700
INACCURACY_RATE = 2.0
HIGH_DELAY_PERC_MIN = 0.00
HIGH_DELAY_PERC_MAX = 0.02


STABILIZE_REQUESTS_COUNT = 200
PROBE_REQUESTS_COUNT = 800


@pytest.fixture(scope='module')
def create_schedulers(request, nodes):
    """Setup/teardown for network interface scheduler."""
    for node in nodes:
        network.add_scheduler(node.host)

    def fin():
        for node in nodes:
            network.del_scheduler(node.host)

    request.addfinalizer(fin)


@pytest.fixture(scope='function')
def session(nodes):
    """Returns prepared elliptics.Session."""
    elog = elliptics.Logger(LOG_FILE, 4)
    ecfg = elliptics.Config()
    # Set DNET_CFG_MIX_STATES flag
    ecfg.flags |= elliptics.config_flags.mix_stats
    client_node = elliptics.Node(elog, ecfg)
    for node in nodes:
        client_node.add_remote(node.host, node.port, socket.AF_INET)

    elliptics_session = elliptics.Session(client_node)

    # Get uniq groups from nodes list
    groups = list({node.group for node in nodes})
    elliptics_session.set_groups(groups)

    return elliptics_session


@pytest.fixture(scope='function')
def key(session):
    """Writes and returns a key."""
    key, data = utils.get_key_and_data(DATA_LENGTH, randomize_len=False)
    session.write_data(key, data).wait()
    return key


def do_requests(session, nodes, key, requests_number):
    """Does specified amount of READ requests."""
    requests_count = {socket.gethostbyname(node.host): 0
                      for node in nodes}

    for _ in xrange(requests_number):
        result = session.read_data(key).get().pop()
        requests_count[result.address.host] += 1

    return requests_count


@pytest.fixture(scope='module', params=["------", "+-----", "+++++-"])
def case(request, nodes, create_schedulers):
    """Returns test cases.

    Test case has following format:

        {
          "delay": <delay>,
          "expected_min": <high_delay_perc_min>,
          "expected_max": HIGH_DELAY_PERC_MIN
        }

    """
    # Calculate expected requests percentage maximum and minimum for nodes with low delay
    low_delay_nodes_count = request.param.count('-')
    low_delay_percentage = 1.0 / low_delay_nodes_count
    low_delay_perc_min = low_delay_percentage / INACCURACY_RATE
    low_delay_perc_max = low_delay_percentage * INACCURACY_RATE
    low_delay_perc_max = low_delay_perc_max if low_delay_perc_max < 1.0 else 1.0

    new_case = {}
    for i, delay_level in enumerate(request.param):
        address = socket.gethostbyname(nodes[i].host)
        if delay_level == '+':
            delay = HIGH_DELAY
            new_case[address] = {"delay": delay,
                                 "expected_min": HIGH_DELAY_PERC_MIN,
                                 "expected_max": HIGH_DELAY_PERC_MAX}
        else:
            delay = LOW_DELAY
            new_case[address] = {"delay": delay,
                                 "expected_min": low_delay_perc_min,
                                 "expected_max": low_delay_perc_max}
        network.set_networking_delay(nodes[i].host, delay)

    return new_case


@pytest.fixture(scope='function')
def requests_count(case, session, nodes, key):
    """Returns statistics about numbers of requests which were send to each node."""
    # Do some requests to stabilize weights
    do_requests(session, nodes, key, STABILIZE_REQUESTS_COUNT)
    # Probe requests
    return do_requests(session, nodes, key, PROBE_REQUESTS_COUNT)


def test_mix_states(case, requests_count):
    """Testing that weight calculation algorithm is using.

    When `elliptics.config_flags.mix_states` flag set, requests (for READ
    operation) will be distributed uniformly between nodes which respond faster.

    """
    logger.info("Case: {}\nRequests count: {}\n".format(json.dumps(case, indent=4), requests_count))

    requests_sum = sum(requests_count.values())

    for host in case:
        expected_requests_min = int(requests_sum * case[host]["expected_min"])
        expected_requests_max = int(requests_sum * case[host]["expected_max"])

        assert_that(requests_count[host], all_of(greater_than_or_equal_to(expected_requests_min),
                                                 less_than_or_equal_to(expected_requests_max)),
                    "Requests count for host ({}) mismatch.".format(host))
