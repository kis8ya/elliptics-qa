import pytest
import time
import random
import elliptics

from hamcrest import assert_that, calling, raises

from test_helper.elliptics_testhelper import EllipticsTestHelper, nodes
from test_helper.utils import get_key_and_data
from test_helper.logging_tests import logger

@pytest.fixture(scope='module')
def client(pytestconfig, nodes):
    """Prepares elliptics session with custom timeouts."""
    wait_timeout = pytestconfig.option.wait_timeout
    check_timeout = pytestconfig.option.check_timeout
    client = EllipticsTestHelper(nodes,
                                 wait_timeout=wait_timeout,
                                 check_timeout=check_timeout)
    return client

#TODO: move this function to lib/utils.py
def drop_groups(client, nodes, group_list):
    groups_to_drop = {}
    for g in group_list:
        groups_to_drop[g] = [n for n in nodes if n.group == g]

    for n in [i for v in groups_to_drop.values() for i in v]:
        client.drop_node(n)

    return groups_to_drop

@pytest.fixture(scope='module')
def key(client):
    """Writes data and returns key."""
    key, data = get_key_and_data()
    client.write_data_sync(key, data)
    return key

@pytest.fixture(scope='module')
def unavailable_groups(request, client, nodes):
    """Drops nodes from half groups and returns these groups."""
    groups_count = len(client.groups)
    groups_count_to_drop = (groups_count+1) / 2
    groups_to_drop = random.sample(client.groups, groups_count_to_drop)
    dropped_groups = drop_groups(client, nodes, groups_to_drop)

    def fin():
        client.resume_all_nodes()
        time.sleep(60)

    request.addfinalizer(fin)

    return dropped_groups

@pytest.fixture(scope='module')
def available_groups(client, nodes, unavailable_groups):
    """Returns available groups."""
    available_groups = {}
    available_groups_list = [g for g in client.groups if g not in unavailable_groups.keys()]
    for g in available_groups_list:
        available_groups[g] = [n for n in nodes if n.group == g]
    return available_groups

@pytest.mark.keepalive
def test_keepalive(client, key, unavailable_groups, available_groups):
    """Testing that reading files from unavailable groups will raise the
    elliptics.Error("...No such device or address: -6") after certain time of downtime
    """
    for g in available_groups.keys():
        client.read_data_from_groups_sync(key, [g])

    for g in unavailable_groups.keys():
        assert_that(calling(client.read_data_from_groups_sync).with_args(key, [g]),
                    raises(elliptics.TimeoutError))

    wait_time = 180
    logger.info("Waiting for {0} seconds to update client's routes list\n".format(wait_time))
    time.sleep(wait_time)

    for g in available_groups.keys():
        client.read_data_from_groups_sync(key, [g])

    for g in unavailable_groups.keys():
        assert_that(calling(client.read_data_from_groups_sync).with_args(key, [g]),
                    raises(elliptics.Error, client.error_info.AddrNotExists))

    client.resume_all_nodes()
    time.sleep(60)

    for g in client.groups:
        client.read_data_from_groups_sync(key, [g])
