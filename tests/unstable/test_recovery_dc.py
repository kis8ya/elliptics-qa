import pytest
import elliptics
import subprocess
import random
import json

from hamcrest import assert_that, raises, calling, equal_to

from test_helper.elliptics_testhelper import EllipticsTestHelper, nodes
from test_helper.utils import get_sha1, get_key_and_data
from test_helper.logging_tests import logger

@pytest.fixture(scope='module')
def client(nodes):
    """Prepares elliptics session with long timeouts."""
    client = EllipticsTestHelper(nodes=nodes, wait_timeout=25, check_timeout=30, logging_level=0)
    return client


def write_files(client, files_number, file_size):
    """Writes files with preset client and specified files parameters: numbers and size."""
    logger.info("Started writing {0} files\n".format(files_number))

    key_list = []
    for i in xrange(files_number):
        key, data = get_key_and_data(file_size, randomize_len=False)

        client.write_data_sync(key, data)
        key_list.append(key)
        logger.info("\r{0}/{1}".format(i + 1, files_number))

    logger.info("\nFinished writing files\n")

    return key_list


@pytest.fixture(scope='module')
def dropped_groups(pytestconfig, client):
    """Returns a list of dropped groups.

    There are only "good" keys will be accessible in these groups.
    "Bad" and "broken" keys will not be written to them.

    """
    if pytestconfig.option.dropped_groups:
        groups = json.load(open(pytestconfig.option.dropped_groups))
    else:
        groups_count = len(client.groups)
        dropped_groups_count = (groups_count + 1) / 2
        groups = random.sample(client.groups, dropped_groups_count)

    return groups


@pytest.fixture(scope='function')
def good_keys(pytestconfig, client):
    """Returns list of "good" keys."""
    if pytestconfig.option.good_keys_path:
        good_keys = json.load(open(pytestconfig.option.good_keys_path))
        good_keys = [str(k) for k in good_keys]
    else:
        good_keys = write_files(client,
                                pytestconfig.option.good_files_number,
                                pytestconfig.option.files_size)
    return good_keys


@pytest.fixture(scope='function')
def bad_keys(request, pytestconfig, client, dropped_groups):
    """Returns list of "bad" keys."""
    full_groups_list = client.groups

    if pytestconfig.option.bad_keys_path:
        bad_keys = json.load(open(pytestconfig.option.bad_keys_path))
        bad_keys = [str(k) for k in bad_keys]
    else:
        # "Bad" keys will be written in all groups except groups from dropped_groups
        client.set_groups([g for g in full_groups_list if g not in dropped_groups])
        bad_keys = write_files(client,
                               pytestconfig.option.bad_files_number,
                               pytestconfig.option.files_size)

    def fin():
        """Restores client's groups."""
        client.set_groups(full_groups_list)

    request.addfinalizer(fin)

    return bad_keys


@pytest.fixture(scope='function')
def broken_keys(request, pytestconfig, client, dropped_groups):
    """Returns list of "broken" keys."""
    full_groups_list = client.groups

    if pytestconfig.option.broken_keys_path:
        broken_keys = json.load(open(pytestconfig.option.broken_keys_path))
        broken_keys = [str(k) for k in broken_keys]
    else:
        # "Broken" keys will be written in all groups except groups from dropped_groups
        client.set_groups([g for g in full_groups_list if g not in dropped_groups])
        broken_keys = write_files(client,
                                  pytestconfig.option.broken_files_number,
                                  pytestconfig.option.files_size)

    def fin():
        """Restores client's groups."""
        client.set_groups(full_groups_list)

    request.addfinalizer(fin)

    return broken_keys


@pytest.fixture(scope='function')
def dump_file(client, bad_keys):
    """Writes id of keys to a dump file and returns the file name."""
    ids = [str(client.transform(k)) for k in bad_keys]
    file_name = "id_dump"
    with open(file_name, "w") as f:
        f.write("\n".join(ids))
    return file_name


def test_dump_file(client, nodes, good_keys, bad_keys, broken_keys, dropped_groups, dump_file):
    node = random.choice(nodes)
    cmd = ["dnet_recovery",
           "--remote", "{}:{}:2".format(node.host, node.port),
           "--groups", ','.join([str(g) for g in client.groups]),
           "--dump-file", dump_file,
           "dc"]
    logger.info("{}\n".format(cmd))

    retcode = subprocess.call(cmd)
    assert retcode == 0, "{} retcode = {}".format(cmd, retcode)

    logger.info('Checking recovered keys...\n')
    for k in bad_keys:
        for g in client.groups:
            result = client.read_data_from_groups_sync(k, [g])[0]
            assert_that(k, equal_to(get_sha1(result.data)))

    logger.info('Checking "good" keys...\n')
    for k in good_keys:
        for g in client.groups:
            result = client.read_data_from_groups_sync(k, [g])[0]
            assert_that(k, equal_to(get_sha1(result.data)))

    logger.info('Check "broken" keys...\n')
    available_groups = [g for g in client.groups if g not in dropped_groups]
    for k in broken_keys:
        for g in available_groups:
            result = client.read_data_from_groups_sync(k, [g])[0]
            assert_that(k, equal_to(get_sha1(result.data)))

    for k in broken_keys:
        for g in dropped_groups:
            assert_that(calling(client.read_data_from_groups_sync).with_args(k, [g]),
                        raises(elliptics.NotFoundError))
