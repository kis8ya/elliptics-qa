import pytest
import time
import subprocess
import random
import elliptics

from collections import defaultdict
from hamcrest import assert_that, raises, calling, equal_to

import test_helper.elliptics_testhelper as et
from test_helper.elliptics_testhelper import nodes
from test_helper.utils import MB
from test_helper.logging_tests import logger
from conftest import RingPartitioning, key_and_data, wait_routes_list_update

def drop_nodes(client, nodes, number):
    """Drops some randomly chosen nodes."""
    nodes_to_drop = random.sample(nodes, number)

    for n in nodes_to_drop:
        client.drop_node(n)

    return nodes_to_drop

def write_data_when_dropped(client, nodes, files_number, files_size, nodes_number):
    """Prepares test data for dc-recovery tests.

    At first it drops nodes. Then it writes data and collect written keys
    to two dictionaries (with "good" and "bad" keys). And at the end it resumes the nodes.

    """
    full_ring_partitioning = RingPartitioning(client, nodes)

    # Drop nodes
    dropped_nodes = drop_nodes(client, nodes, nodes_number)
    wait_routes_list_update(client)

    new_ring_partitioning = RingPartitioning(client, nodes)

    # Write data
    good_keys = defaultdict(list)
    bad_keys = defaultdict(list)
    data_size = 0
    logger.info("Started writing {0} files\n".format(files_number))
    for i in xrange(files_number):
        key, data = key_and_data(files_size)
        elliptics_id =  client.transform(key)

        client.write_data_sync(key, data)

        current_host = new_ring_partitioning.get_hostname_by_id(elliptics_id)
        proper_host = full_ring_partitioning.get_hostname_by_id(elliptics_id)
        if current_host == proper_host:
            good_keys[current_host].append(key)
        else:
            bad_keys[current_host].append(key)

        data_size += len(data)

        logger.info("\r{0}/{1}".format(i + 1, files_number))
    logger.info("\nFinished writing files\n")

    bad_key_count = sum([len(v) for v in bad_keys.values()])
    logger.info("with dropped {0} nodes there are {1}/{2} bad keys\ndata size: {3} MB\n".format(
            [n.host for n in dropped_nodes], bad_key_count, files_number, data_size / MB))

    # Resume nodes
    for n in dropped_nodes:
        client.resume_node(n)
    wait_routes_list_update(client)

    return good_keys, bad_keys, dropped_nodes, new_ring_partitioning

@pytest.fixture(scope='function')
def write_data_when_two_dropped(request, client, nodes, files_number, files_size):
    """Prepares test data for dc-recovery test when there were two groups dropped."""
    def fin():
        client.resume_all_nodes()
        time.sleep(60)
        
    request.addfinalizer(fin)

    return write_data_when_dropped(client, nodes, files_number, files_size, 2)

@pytest.mark.merge
def test_one_node_option(client, nodes, write_data_when_two_dropped):
    """Tests that 'dnet_recovery --remote ... --one-node merge'
    will recover proper keys from every node
    (even this node was dropped and has no keys to recover)
    when there were 2 dropped nodes
    """
    good_keys, bad_keys, dropped_nodes, dropped_ring_partitioning = write_data_when_two_dropped
    ring_partitioning = RingPartitioning(client, nodes)
    available_nodes = [n for n in nodes if n not in dropped_nodes]

    # check that "good" keys are accessible
    for key_list in good_keys.values():
        for k in key_list:
            client.read_data_sync(k)
                
    # check that "bad" keys are not accessible
    for key_list in bad_keys.values():
        for k in key_list:
            assert_that(calling(client.read_data_sync).with_args(k),
                        raises(elliptics.NotFoundError))

    # Run dnet_recovery --one-node merge for nodes with keys to recover
    bad_key_list = [i for v in bad_keys.values() for i in v]
    recovered_keys = []
    for node in available_nodes:
        cmd = ["dnet_recovery",
               "--groups", ','.join(map(str, client.groups)),
               "--one-node", "{0}:{1}:2".format(node.host, node.port),
               "merge"]

        logger.info("{0}\n".format(cmd))
        retcode = subprocess.call(cmd)
        assert retcode == 0, "{0} retcode = {1}".format(cmd, retcode)

        key_list = []
        for k in bad_key_list:
            elliptics_id = client.transform(k)
            if dropped_ring_partitioning.is_my_id(node.host, elliptics_id):
                key_list.append(k)
                bad_key_list.remove(k)

        # check that keys are accessible
        for k in key_list:
            client.read_data_sync(k)

        recovered_keys.extend(key_list)

        # check that "bad" keys are not accessible yet
        for k in bad_key_list:
            assert_that(calling(client.read_data_sync).with_args(k),
                        raises(elliptics.NotFoundError))

    # check accessibility for all keys
    good_key_list = [i for v in good_keys.values() for i in v]
    all_keys = recovered_keys + good_key_list
    for k in all_keys:
        data = client.read_data_sync(k).pop().data
        assert_that(k, equal_to(et.utils.get_sha1(data)))

@pytest.mark.merge
def test_merge_add_two_nodes(client, nodes, write_data_when_two_dropped):
    """Tests that 'dnet_recovery --remote ... merge'
    will recover all keys when there were 2 dropped nodes
    """
    good_keys, bad_keys, dropped_nodes, dropped_ring_partitioning = write_data_when_two_dropped

    # check that "good" keys are accessible
    for key_list in good_keys.values():
        for k in key_list:
            client.read_data_sync(k)
                
    # check that "bad" keys are not accessible
    for key_list in bad_keys.values():
        for k in key_list:
            assert_that(calling(client.read_data_sync).with_args(k),
                        raises(elliptics.NotFoundError))

    # Run dnet_recovery merge
    node = random.choice(nodes)
    cmd = ["dnet_recovery",
           "--remote", "{0}:{1}:2".format(node.host, node.port),
           "--groups", ','.join(map(str, client.groups)),
           "merge"]
    logger.info("{0}\n".format(cmd))
    retcode = subprocess.call(cmd)
    assert retcode == 0, "{0} retcode = {1}".format(cmd, retcode)

    # check all keys and data
    good_keys_list = [k for v in good_keys.values() for k in v]
    bad_keys_list = [k for v in bad_keys.values() for k in v]
    for k in good_keys_list + bad_keys_list:
        data = client.read_data_sync(k).pop().data
        assert_that(k, equal_to(et.utils.get_sha1(data)))
