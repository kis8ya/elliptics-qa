import pytest
import time
import subprocess
import random
import math

from collections import defaultdict
from hamcrest import assert_that, raises, calling, equal_to

import elliptics

import elliptics_testhelper as et

from elliptics_testhelper import nodes
from matchers import hasitems
from logging_tests import logger
from conftest import RingPartitioning, key_and_data, wait_routes_list_update

def drop_groups(client, nodes, group_list):
    """Drops nodes from given elliptics groups."""
    groups_to_drop = {}
    for g in group_list:
        groups_to_drop[g] = [n for n in nodes if n.group == g]

    for n in [i for v in groups_to_drop.values() for i in v]:
        client.drop_node(n)

    return groups_to_drop

@pytest.fixture(scope='function')
def write_indexes_when_groups_dropped(request, client, nodes, files_number, files_size):
    """Drops half groups, writes data then resume all nodes from these groups."""
    # Generate 5 random indexes
    indexes = map(str, random.sample(xrange(10000000), 5))

    # Write "good" keys
    good_keys = defaultdict(list)
    key_count = 127
    logger.info("Started writing {0} files\n".format(key_count))
    for i in xrange(key_count):
        key, data = key_and_data(files_size)
        indexes_count = random.randint(1, len(indexes) - 1)
        key_indexes = random.sample(indexes, indexes_count)
        index_data = ["idata_{0}".format(k) for k in key_indexes]

        client.write_data_sync(key, data)
        client.set_indexes(key, key_indexes, index_data).get()
        good_keys[key].extend(key_indexes)

        logger.info("\r{0}/{1}".format(i + 1, key_count))
    logger.info("\nFinished writing files\n")

    # Drop nodes
    groups_count = len(client.groups)
    groups_count_to_drop = int(math.ceil(groups_count / 2.0))
    groups_to_drop = random.sample(client.groups, groups_count_to_drop)
    dropped_groups = drop_groups(client, nodes, groups_to_drop)

    wait_routes_list_update(client)

    # Write "bad" keys
    bad_keys = defaultdict(list)
    logger.info("Started writing {0} files\n".format(files_number))
    for i in xrange(files_number):
        key, data = key_and_data(files_size)
        indexes_count = random.randint(1, len(indexes) - 1)
        key_indexes = random.sample(indexes, indexes_count)
        index_data = ["idata_{0}".format(k) for k in key_indexes]

        client.write_data_sync(key, data)
        client.set_indexes(key, key_indexes, index_data).get()
        bad_keys[key].extend(key_indexes)

        logger.info("\r{0}/{1}".format(i + 1, files_number))
    logger.info("\nFinished writing files\n")

    for n in [i for v in dropped_groups.values() for i in v]:
        client.resume_node(n)

    wait_routes_list_update(client)

    def fin():
        client.resume_all_nodes()
        time.sleep(60)
        
    request.addfinalizer(fin)

    return good_keys, bad_keys, dropped_groups, indexes

@pytest.mark.dc
def test_one_node_dc_with_indexes(client, nodes, write_indexes_when_groups_dropped):
    """Tests that 'dnet_recovery --remote ... --one-node dc'
    will recover all keys for every dropped group
    when there were half groups dropped
    """
    good_keys, bad_keys, dropped_groups, indexes = write_indexes_when_groups_dropped
    bad_groups = [int(g) for g in dropped_groups.keys()]
    ring_partitioning = RingPartitioning(client, nodes)

    # check that "good" keys are accessible in all groups
    for g in client.groups:
        for k in good_keys.keys():
            client.read_data_from_groups_sync(k, [g])

    # check that "bad" keys are not accessible in "dropped" groups
    for k in bad_keys.keys():
        assert_that(calling(client.read_data_from_groups_sync).with_args(k, bad_groups),
                    raises(elliptics.NotFoundError))

    # check that recovered keys are accessible in all groups
    good_groups = [g for g in client.groups if g not in bad_groups]
    for g in good_groups:
        for k in bad_keys.keys():
            client.read_data_from_groups_sync(k, [g])

    test_group = random.choice(client.groups)
    test_nodes = [n for n in nodes if n.group == test_group]
    recovered_keys = []
    for node in test_nodes:
        cmd = ["dnet_recovery",
               "--remote", "{0}:{1}:2".format(node.host, node.port),
               "--one-node",
               "--groups", ','.join(map(str, client.groups)),
               "dc"]
        logger.info("{0}\n".format(cmd))
        subprocess.call(cmd)

        for k, v in bad_keys.items():
            elliptics_id = client.transform(k)
            if ring_partitioning.is_my_id(node.host, elliptics_id):
                good_keys[k].extend(v)
                del bad_keys[k]

        # check that bad keys are not accessible yet in "dropped" groups
        for g in dropped_groups.keys():
            for k in bad_keys.keys():
                assert_that(calling(client.read_data_from_groups_sync).with_args(k, [g]),
                            raises(elliptics.NotFoundError))

        # check that "good" keys are accessible in all groups
        for g in client.groups:
            for k in good_keys.keys():
                client.read_data_from_groups_sync(k, [g])

    # check all keys
    for g in client.groups:
        for k in good_keys.keys():
            data = client.read_data_from_groups_sync(k, [g]).pop().data
            assert_that(k, equal_to(et.utils.get_sha1(data)))

    # check indexes
    for i in indexes:
        result = client.find_all_indexes([i]).get()
        result_ids = [r.id for r in result]
        ids_with_index = [client.transform(k) for k, v in good_keys.items() if i in v]
        assert_that(result_ids, hasitems(*ids_with_index))

@pytest.mark.dc
def test_dc_with_indexes(client, nodes, write_indexes_when_groups_dropped):
    """Tests that 'dnet_recovery --remote ... dc'
    will recover all keys for every dropped group
    when there were half groups dropped
    """
    good_keys, bad_keys, dropped_groups, indexes = write_indexes_when_groups_dropped
    bad_groups = [int(g) for g in dropped_groups.keys()]
    ring_partitioning = RingPartitioning(client, nodes)

    # check that "good" keys are accessible in all groups
    for g in client.groups:
        for k in good_keys.keys():
            client.read_data_from_groups_sync(k, [g])

    # check that "bad" keys are not accessible in "dropped" groups
    for k in bad_keys.keys():
        assert_that(calling(client.read_data_from_groups_sync).with_args(k, bad_groups),
                    raises(elliptics.NotFoundError))

    # check that recovered keys are accessible in all groups
    good_groups = [g for g in client.groups if g not in bad_groups]
    for g in good_groups:
        for k in bad_keys.keys():
            client.read_data_from_groups_sync(k, [g])

    n = random.randint(0, len(nodes) - 1)
    cmd = ["dnet_recovery",
           "--remote", "{0}:{1}:2".format(nodes[n].host, nodes[n].port),
           "--groups", ','.join(map(str, client.groups)),
           "dc"]
    logger.info("{0}\n".format(cmd))
    subprocess.call(cmd)

    for k, v in bad_keys.items():
        good_keys[k].extend(v)

    # check all keys
    for g in client.groups:
        for k in good_keys.keys():
            data = client.read_data_from_groups_sync(k, [g]).pop().data
            assert_that(k, equal_to(et.utils.get_sha1(data)))

    # check indexes
    for i in indexes:
        result = client.find_all_indexes([i]).get()
        result_ids = [r.id for r in result]
        ids_with_index = [client.transform(k) for k, v in good_keys.items() if i in v]
        assert_that(result_ids, hasitems(*ids_with_index))

@pytest.mark.dc
def test_nprocess_dc_with_indexes(client, nodes, write_indexes_when_groups_dropped):
    """Tests that 'dnet_recovery --nprocess=3 --remote ... dc'
    will recover all keys for every dropped group
    when there were half groups dropped
    """
    good_keys, bad_keys, dropped_groups, indexes = write_indexes_when_groups_dropped
    bad_groups = [int(g) for g in dropped_groups.keys()]
    ring_partitioning = RingPartitioning(client, nodes)

    # check that "good" keys are accessible in all groups
    for g in client.groups:
        for k in good_keys.keys():
            client.read_data_from_groups_sync(k, [g])

    # check that "bad" keys are not accessible in "dropped" groups
    for k in bad_keys.keys():
        assert_that(calling(client.read_data_from_groups_sync).with_args(k, bad_groups),
                    raises(elliptics.NotFoundError))

    # check that recovered keys are accessible in all groups
    good_groups = [g for g in client.groups if g not in bad_groups]
    for g in good_groups:
        for k in bad_keys.keys():
            client.read_data_from_groups_sync(k, [g])

    n = random.randint(0, len(nodes) - 1)
    cmd = ["dnet_recovery",
           "--nprocess", "3",
           "--remote", "{0}:{1}:2".format(nodes[n].host, nodes[n].port),
           "--groups", ','.join(map(str, client.groups)),
           "dc"]
    logger.info("{0}\n".format(cmd))
    subprocess.call(cmd)

    for k, v in bad_keys.items():
        good_keys[k].extend(v)

    # check all keys
    for g in client.groups:
        for k in good_keys.keys():
            data = client.read_data_from_groups_sync(k, [g]).pop().data
            assert_that(k, equal_to(et.utils.get_sha1(data)))

    # check indexes
    for i in indexes:
        result = client.find_all_indexes([i]).get()
        result_ids = [r.id for r in result]
        ids_with_index = [client.transform(k) for k, v in good_keys.items() if i in v]
        assert_that(result_ids, hasitems(*ids_with_index))
