import pytest
import elliptics
import socket
import json

from collections import defaultdict
from hamcrest import assert_that, all_of, greater_than_or_equal_to, less_than_or_equal_to

import mix_states_utils

from mix_states_utils import RequestsCounter
from test_helper import utils, network
from test_helper.elliptics_testhelper import nodes
from test_helper.logging_tests import logger

# Logging file for elliptics client
LOG_FILE = "elliptics_client.log"
# Data length for key which will be used for test requests
DATA_LENGTH = 1
# Networking delays
LOW_DELAY = 0
HIGH_DELAY = 700
# Expected taken time (in microseconds) for READ operations on nodes with low delay
LOW_DELAY_EXPECTED_TIME = 5000
# A rate to calculate a confidence interval for test check
INACCURACY_RATE = 2.0
# A confidence interval (in percentage) for nodes with high delay
HIGH_DELAY_PERC_MIN = 0.00
HIGH_DELAY_PERC_MAX = 0.02
# Number of requests to stabilize weights
STABILIZE_REQUESTS_COUNT = 50
# Number of requests in one sample for the test to check
SAMPLE_REQUESTS_COUNT = 100
# Number of samples
SAMPLES_COUNT = 10
# Number of retries for stabilizing weights
STABILIZING_RETRY_NUMBER_MAX = 1000
# Number of retries for collecting statistics
STATISTICS_RETRY_NUMBER_MAX = 1000000


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
    """Returns prepared elliptics.Session with set `config_flags.mix_states` flag."""
    elog = elliptics.Logger(LOG_FILE, elliptics.log_level.debug)
    ecfg = elliptics.Config()
    # Set DNET_CFG_MIX_STATES flag
    ecfg.flags |= elliptics.config_flags.mix_states
    client_node = elliptics.Node(elog, ecfg)
    
    addresses = [elliptics.Address(node.host, node.port, socket.AF_INET)
                 for node in nodes]
    client_node.add_remotes(addresses)

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


@pytest.fixture(scope='module', params=["------", "+-----", "+++++-"])
def case(request, nodes, create_schedulers):
    """Returns test cases.

    Test case has following format:

        {
          "delay": <delay>,
          "expected_min": <percentage minimum>,
          "expected_max": <percentage maximum>
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
def requests_count(case, session, key):
    """Returns statistics about numbers of requests that were send to each node."""
    statistics = defaultdict(int)
    data_samples_collected = 0
    retry_number = 0
    # Prepare parameters to check a READ-transaction time
    logged_destructions = mix_states_utils.get_logged_destructions(session, LOG_FILE)
    trans_checker_params = RequestsCounter.TransCheckerParams(logged_destructions, case,
                                                              LOW_DELAY, LOW_DELAY_EXPECTED_TIME)

    while retry_number < STATISTICS_RETRY_NUMBER_MAX and \
          data_samples_collected < SAMPLES_COUNT:
        # Stabilize weight before sample
        mix_states_utils.do_requests_with_retry(session, key, STABILIZE_REQUESTS_COUNT,
                                                STABILIZING_RETRY_NUMBER_MAX, trans_checker_params)

        sample = mix_states_utils.do_requests_with_retry(session, key, SAMPLE_REQUESTS_COUNT,
                                                         1, trans_checker_params)

        if sample is None:
            retry_number += 1
        else:
            # Collecting statistics
            for host, requests_count in sample.items():
                statistics[host] += requests_count
            data_samples_collected += 1

    if data_samples_collected == SAMPLES_COUNT:
        return statistics
    else:
        raise RuntimeError("Retries count ({}) exceeded while was trying to collect statistics."
                           .format(retry_number))


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
