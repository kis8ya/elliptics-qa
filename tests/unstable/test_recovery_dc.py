"""Elliptics dc recovery tests.

These tests are testing dc recovery for elliptics recovery script (`dnet_recovery`).
Tests are using following types of keys:

* good keys (accessible from all groups);
* bad keys (accessible from several groups and will be recovered);
* broken keys (accessible from several groups and will not be recovered).

Running tests
-------------

The tests split up their functionality into two ways to run:

1. Run tests when they will prepare all data on the run.
1. Specify files with information about prepared data.

Example of running tests with data preparation on the run (for more information
see `py.test --help`):

    PYTHONPATH=./lib py.test -v -s \
    --node=server-1:1025:1 \
    --node=server-1:1026:1 \
    --node=server-1:1027:1 \
    --node=server-2:1025:2 \
    --node=server-2:1026:2 \
    --node=server-2:1027:2 \
    --node=server-3:1025:3 \
    --node=server-3:1026:3 \
    --node=server-3:1027:3 \
    --good-files-number=70 \
    --bad-files-number=100 \
    --broken-files-number=50 \
    --files-size=102400 \
    tests/unstable/

Example of running tests with prepared files with information about written data:

    PYTHONPATH=./lib py.test -v -s \
    --node=server-1:1025:1 \
    --node=server-1:1026:1 \
    --node=server-1:1027:1 \
    --node=server-2:1025:2 \
    --node=server-2:1026:2 \
    --node=server-2:1027:2 \
    --node=server-3:1025:3 \
    --node=server-3:1026:3 \
    --node=server-3:1027:3 \
    --good-keys-path=./good_keys \
    --bad-keys-path=./bad_keys \
    --broken-keys-path=./broken_keys \
    tests/unstable/

"""

import pytest
import elliptics
import subprocess
import random
import json

from hamcrest import assert_that, raises, calling, equal_to, described_as

from test_helper.elliptics_testhelper import EllipticsTestHelper, nodes
from test_helper.utils import get_sha1, get_key_and_data
from test_helper.logging_tests import logger


def write_files(session, files_number, file_size):
    """Writes files with preset session and specified files parameters: numbers and size."""
    logger.info("Started writing {0} files\n".format(files_number))

    key_list = []
    for i in xrange(files_number):
        key, data = get_key_and_data(file_size, randomize_len=False)

        session.write_data(key, data).wait()
        key_list.append(key)
        logger.info("\r{0}/{1}".format(i + 1, files_number))

    logger.info("\nFinished writing files\n")

    return key_list


@pytest.fixture(scope='module')
def dropped_groups(pytestconfig, session):
    """Returns a list of dropped groups.

    There are only "good" keys will be accessible in these groups.
    "Bad" and "broken" keys will not be written to them.

    """
    if pytestconfig.option.dropped_groups:
        groups = json.load(open(pytestconfig.option.dropped_groups))
    else:
        groups_count = len(session.groups)
        dropped_groups_count = (groups_count + 1) / 2
        groups = random.sample(session.groups, dropped_groups_count)

    return groups


@pytest.fixture(scope='function')
def good_keys(pytestconfig, session):
    """Returns list of "good" keys."""
    if pytestconfig.option.good_keys_path:
        good_keys = json.load(open(pytestconfig.option.good_keys_path))
        good_keys = [str(k) for k in good_keys]
    else:
        good_keys = write_files(session,
                                pytestconfig.option.good_files_number,
                                pytestconfig.option.files_size)
    return good_keys


@pytest.fixture(scope='function')
def bad_keys(pytestconfig, session, dropped_groups):
    """Returns list of "bad" keys."""
    if pytestconfig.option.bad_keys_path:
        bad_keys = json.load(open(pytestconfig.option.bad_keys_path))
        bad_keys = [str(k) for k in bad_keys]
    else:
        restricted_session = session.clone()
        # "Bad" keys will be written in all groups except groups from dropped_groups
        restricted_session.set_groups([g for g in restricted_session.groups if g not in dropped_groups])
        bad_keys = write_files(restricted_session,
                               pytestconfig.option.bad_files_number,
                               pytestconfig.option.files_size)

    return bad_keys


@pytest.fixture(scope='function')
def broken_keys(pytestconfig, session, dropped_groups):
    """Returns list of "broken" keys."""
    if pytestconfig.option.broken_keys_path:
        broken_keys = json.load(open(pytestconfig.option.broken_keys_path))
        broken_keys = [str(k) for k in broken_keys]
    else:
        restricted_session = session.clone()
        # "Broken" keys will be written in all groups except groups from dropped_groups
        restricted_session.set_groups([g for g in restricted_session.groups
                                       if g not in dropped_groups])
        broken_keys = write_files(restricted_session,
                                  pytestconfig.option.broken_files_number,
                                  pytestconfig.option.files_size)

    return broken_keys


@pytest.fixture(scope='function')
def dump_file(session, bad_keys):
    """Writes id of keys to a dump file and returns the file name."""
    ids = [str(session.transform(k)) for k in bad_keys]
    file_name = "id_dump"
    with open(file_name, "w") as f:
        f.write("\n".join(ids))
    return file_name


def test_dump_file(session, nodes, good_keys, bad_keys, broken_keys, dropped_groups, dump_file):
    """Testing `dnet_recovery` with `--dump-file` option."""
    node = random.choice(nodes)
    cmd = ["dnet_recovery",
           "--remote", "{}:{}:2".format(node.host, node.port),
           "--groups", ','.join([str(g) for g in session.groups]),
           "--dump-file", dump_file,
           "dc"]
    logger.info("{}\n".format(cmd))

    retcode = subprocess.call(cmd)
    assert retcode == 0, "{} retcode = {}".format(cmd, retcode)

    logger.info('Checking recovered keys...\n')
    for k in bad_keys:
        for g in session.groups:
            result = session.read_data_from_groups(k, [g]).get()[0]
            assert_that(k, equal_to(get_sha1(result.data)),
                        "The recovered data mismatch by sha1 hash")

    logger.info('Checking "good" keys...\n')
    for k in good_keys:
        for g in session.groups:
            result = session.read_data_from_groups(k, [g]).get()[0]
            assert_that(k, equal_to(get_sha1(result.data)),
                        "After recovering the data mismatch by sha1 hash")

    logger.info('Check "broken" keys...\n')
    available_groups = [g for g in session.groups if g not in dropped_groups]
    for k in broken_keys:
        for g in available_groups:
            result = session.read_data_from_groups(k, [g]).get()[0]
            assert_that(k, equal_to(get_sha1(result.data)),
                        "After recovering the data mismatch by sha1 hash")

    for k in broken_keys:
        for g in dropped_groups:
            async_result = session.read_data_from_groups(k, [g])
            assert_that(calling(async_result.wait),
                        raises(elliptics.NotFoundError))
