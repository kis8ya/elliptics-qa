"""Elliptics dc recovery tests.

These tests are testing dc recovery for elliptics recovery script (`dnet_recovery`).

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
    --consistent-files-number=20000 \
    --inconsistent-files-number=5000 \
    --inconsistent-files-percentage=0.13 \
    --file-size=102400 \
    tests/unstable/test_recovery_dc.py

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
    --consistent-keys-path=./consistent_keys \
    --inconsistent-keys-path=./inconsistent_keys \
    --dropped-groups-path=./dropped_groups
    --inconsistent-files-percentage=0.15 \
    tests/unstable/test_recovery_dc.py

"""

import pytest
import elliptics
import subprocess
import random
import json
import os

from hamcrest import assert_that, raises, calling, equal_to

from test_helper.elliptics_testhelper import nodes
from test_helper.utils import get_sha1, get_testcases, get_testcases_names
from test_helper.logging_tests import logger
from test_helper.matchers import hasitem

import utils


@pytest.fixture(scope='module')
def indexes():
    """Returns randomly generated indexes."""
    indexes_count = 5
    index_length = 20
    return [os.urandom(index_length) for _ in xrange(indexes_count)]


@pytest.fixture(scope='module')
def dropped_groups(pytestconfig, session):
    """Returns a list of dropped groups."""
    if pytestconfig.option.dropped_groups_path:
        groups = json.load(open(pytestconfig.option.dropped_groups))
    else:
        groups_count = len(session.groups)
        dropped_groups_count = (groups_count + 1) / 2
        groups = random.sample(session.groups, dropped_groups_count)

    return groups


@pytest.fixture(scope='module',
                params=get_testcases("testcases_recovery_dc"),
                ids=get_testcases_names("testcases_recovery_dc"))
def recovery(pytestconfig, request, session, nodes, indexes, dropped_groups):
    """Returns a structure of recovery's data."""
    recovery = request.param(pytestconfig.option, session, nodes, dropped_groups, indexes)
    logger.info("{}\n".format(recovery["cmd"]))

    recovery["exitcode"] = subprocess.call(recovery["cmd"])

    return recovery


def test_exit_code(recovery):
    """Testing that `dnet_recovery` will be processed with exit status code = 0."""
    assert_that(recovery["exitcode"], equal_to(0),
                "`dnet_recovery` exited with non-zero exit code: {}\n"
                "Running details: {}".format(recovery["exitcode"], recovery["cmd"]))


def test_consistent_keys(session, recovery):
    """Testing that after recovery operation keys, which were availabe in all groups,
    are still available in all groups and have correct data."""
    for key in recovery["keys"]["consistent"]:
        for group in session.groups:
            result = session.read_data_from_groups(key, [group]).get()[0]
            assert_that(key, equal_to(get_sha1(result.data)),
                        "After recovering the data mismatch by sha1 hash")


def test_recovered_keys(session, recovery):
    """Testing that after recovery operation keys, which had to be recovered,
    are recovered and have correct data."""
    for key in recovery["keys"]["recovered"]:
        for group in session.groups:
            result = session.read_data_from_groups(key, [group]).get()[0]
            assert_that(key, equal_to(get_sha1(result.data)),
                        "The recovered data mismatch by sha1 hash")


def test_inconsistent_keys(session, recovery, dropped_groups):
    """Testing that after recovery operation keys, which were available in some specific groups,
    are still available in these groups and have correct data."""
    available_groups = [group for group in session.groups
                        if group not in dropped_groups]
    for key in recovery["keys"]["inconsistent"]:
        for group in available_groups:
            result = session.read_data_from_groups(key, [group]).get()[0]
            assert_that(key, equal_to(get_sha1(result.data)),
                        "After recovering the data mismatch by sha1 hash")


def test_inconsistent_keys_wrong_access(session, recovery, dropped_groups):
    """Testing that after recovery operation keys, which were available in some specific groups,
    are not available in other groups."""
    for key in recovery["keys"]["inconsistent"]:
        for group in dropped_groups:
            async_result = session.read_data_from_groups(key, [group])
            assert_that(calling(async_result.wait),
                        raises(elliptics.NotFoundError))


def test_indexes_searching(session, recovery, indexes, dropped_groups):
    """Testing that after recovery operation searching by indexes, which were not available
    in some groups before recovery operation, will return all keys for each index in each group."""
    for index in indexes:
        for group in session.groups:
            restricted_session = session.clone()
            restricted_session.set_groups([group])

            result_keys = restricted_session.find_all_indexes([index]).get()
            result_keys_ids = [key.id for key in result_keys]

            expected_keys = utils.get_expected_keys(recovery, group, index, dropped_groups)
            for expected_key in expected_keys:
                assert_that(result_keys_ids, hasitem(session.transform(expected_key)),
                            'Expected key "{}" not found when was searching for index "{}" '
                            'in group {}'.format(expected_key, index, group))


def test_key_index_data(session, recovery, indexes, dropped_groups):
    """Testing that after recovery operation key-index data will be correct for all keys,
    which had to be recovered or were available before recovery operation."""
    for index in indexes:
        for group in session.groups:
            restricted_session = session.clone()
            restricted_session.set_groups([group])

            expected_keys = utils.get_expected_keys(recovery, group, index, dropped_groups)
            for expected_key in expected_keys:
                key_indexes = restricted_session.list_indexes(expected_key).get()
                filtered_data = [index_entry.data for index_entry in key_indexes
                                 if index_entry.index == restricted_session.transform(index)]
                key_index_data = filtered_data[0]
                assert_that(key_index_data, equal_to(utils.index_data_format(expected_key, index)),
                            'Key-index (key = "{}", index = "{}") data mismatch in group {}'
                            .format(expected_key, index, group))
