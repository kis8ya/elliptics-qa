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
    --file-size=102400 \
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
import os

from hamcrest import assert_that, raises, calling, equal_to

from test_helper.elliptics_testhelper import nodes
from test_helper.utils import get_sha1
from test_helper.logging_tests import logger
from test_helper.matchers import hasitem

import utils
import testcases_recovery_dc


@pytest.fixture(scope='module')
def indexes():
    """Returns randomly generated indexes."""
    indexes_count = 5
    index_length = 20
    return [os.urandom(index_length) for _ in xrange(indexes_count)]


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


recovery_dc_testcases = utils.get_testcases(testcases_recovery_dc)


@pytest.fixture(scope='module',
                params=recovery_dc_testcases,
                ids=[case_func.__name__ for case_func in recovery_dc_testcases])
def recovery(pytestconfig, request, session, nodes, indexes, dropped_groups):
    """Returns an object with information about recovery operation."""
    recovery = {
        "cmd": None,
        "exitcode": None,
        "dropped_groups": dropped_groups,
        "keys": {
            "good": utils.get_good_keys(session,
                                        pytestconfig.option.good_keys_path,
                                        pytestconfig.option.good_files_number,
                                        pytestconfig.option.file_size,
                                        indexes),
            "bad": utils.get_bad_keys(session,
                                      pytestconfig.option.bad_keys_path,
                                      pytestconfig.option.bad_files_number,
                                      pytestconfig.option.file_size,
                                      indexes,
                                      dropped_groups),
            "broken": utils.get_broken_keys(session,
                                            pytestconfig.option.broken_keys_path,
                                            pytestconfig.option.broken_files_number,
                                            pytestconfig.option.file_size,
                                            indexes,
                                            dropped_groups)
        }
    }

    recovery = request.param(session, nodes, recovery)
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
    for key in recovery["keys"]["good"]:
        for group in session.groups:
            result = session.read_data_from_groups(key, [group]).get()[0]
            assert_that(key, equal_to(get_sha1(result.data)),
                        "After recovering the data mismatch by sha1 hash")


def test_recovered_keys(session, recovery):
    """Testing that after recovery operation keys, which had to be recovered,
    are recovered and have correct data."""
    for key in recovery["keys"]["bad"]:
        for group in session.groups:
            result = session.read_data_from_groups(key, [group]).get()[0]
            assert_that(key, equal_to(get_sha1(result.data)),
                        "The recovered data mismatch by sha1 hash")


def test_inconsistent_keys(session, recovery):
    """Testing that after recovery operation keys, which were available in some specific groups,
    are still available in these groups and have correct data."""
    available_groups = [group for group in session.groups
                        if group not in recovery["dropped_groups"]]
    for key in recovery["keys"]["broken"]:
        for group in available_groups:
            result = session.read_data_from_groups(key, [group]).get()[0]
            assert_that(key, equal_to(get_sha1(result.data)),
                        "After recovering the data mismatch by sha1 hash")


def test_inconsistent_keys_wrong_access(session, recovery):
    """Testing that after recovery operation keys, which were available in some specific groups,
    are not available in other groups."""
    for key in recovery["keys"]["broken"]:
        for group in recovery["dropped_groups"]:
            async_result = session.read_data_from_groups(key, [group])
            assert_that(calling(async_result.wait),
                        raises(elliptics.NotFoundError))


def test_indexes_return_all_available_keys(session, recovery, indexes):
    """Testing that after recovery operation searching by indexes, which were not available
    in some groups before recovery operation, will return all keys for each index in each group."""
    for index in indexes:
        for group in session.groups:
            restricted_session = session.clone()
            restricted_session.set_groups([group])

            result_keys = restricted_session.find_all_indexes([index]).get()
            result_keys_ids = [key.id for key in result_keys]

            expected_keys = utils.get_expected_keys(recovery, group, index)
            for expected_key in expected_keys:
                assert_that(result_keys_ids, hasitem(session.transform(expected_key)),
                            'Expected key "{}" not found when was searching for index "{}" '
                            'in group {}'.format(expected_key, index, group))


def test_indexes_have_expected_data(session, recovery, indexes):
    """Testing that after recovery operation key-index data will be correct for all keys,
    which had to be recovered or were available before recovery operation."""
    for index in indexes:
        for group in session.groups:
            restricted_session = session.clone()
            restricted_session.set_groups([group])

            expected_keys = utils.get_expected_keys(recovery, group, index)
            for expected_key in expected_keys:
                key_indexes = restricted_session.list_indexes(expected_key).get()
                filtered_data = [index_entry.data for index_entry in key_indexes
                                 if index_entry.index == restricted_session.transform(index)]
                key_index_data = filtered_data[0]
                assert_that(key_index_data, equal_to(utils.index_data_format(expected_key, index)),
                            'Key-index (key = "{}", index = "{}") data mismatch in group {}'
                            .format(expected_key, index, group))
