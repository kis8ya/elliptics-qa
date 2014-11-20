"""Elliptics recovery-dc tests.

These tests are testing recovery-dc for elliptics recovery script (`dnet_recovery`).

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
    --cache-sync-timeout=30 \
    --recovery-timeout=3600 \
    tests/elliptics/recovery/dc/test_recovery_dc.py

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
    --recovery-timeout=86400 \
    tests/elliptics/recovery/dc/test_recovery_dc.py

"""

import pytest
import json
import random
import elliptics

from hamcrest import assert_that, calling, raises, equal_to, is_not

from test_helper import utils
from test_helper.elliptics_testhelper import nodes

from recovery.utils.testrecovery import AbstractTestRecovery


class TestRecoveryDC(AbstractTestRecovery):
    def assert_valid_keys(self, session, recovery, keys_type):
        """Checks specified keys.

        It checks that:
        1. keys are accessible.
        2. keys have correct data.
        
        """
        for key in recovery["keys"][keys_type]:
            for group in session.groups:
                async_read = session.read_data_from_groups(key, [group])

                assert_that(calling(async_read.wait),
                            is_not(raises(elliptics.NotFoundError)),
                            "{} key is not accessible after recovery operation in node, "
                            "where it must be accessible.".format(keys_type))
                
                data_hash = utils.get_sha1(async_read.get()[0].data)

                assert_that(data_hash, equal_to(key),
                            "{} key's data mismatch after recovery operation.".format(keys_type))

    def get_expected_keys(self, recovery, group, index, dropped_groups):
        """Returns a list of all keys for the index that should be available in the group."""
        expected_keys = [key for key, key_indexes in recovery["keys"]["consistent"].items()
                         if index in key_indexes]
        # add recovered keys if their indexes were recovered or we are checking indexes in group
        # which was available and had indexes before recovery operation
        if recovery["recovery_indexes"] or group not in dropped_groups:
            expected_keys.extend([key for key, key_indexes in recovery["keys"]["recovered"].items()
                                  if index in key_indexes])
        if group not in dropped_groups:
            expected_keys.extend([key
                                  for key, key_indexes in recovery["keys"]["inconsistent"].items()
                                  if index in key_indexes])
        return expected_keys
    
    @pytest.fixture(scope='class')
    def dropped(self, pytestconfig, session):
        """Returns a list of dropped groups."""
        if pytestconfig.option.dropped_groups_path:
            groups = json.load(open(pytestconfig.option.dropped_groups_path))
        else:
            groups_count = len(session.groups)
            dropped_groups_count = (groups_count + 1) / 2
            groups = random.sample(session.groups, dropped_groups_count)

        return groups

    @pytest.fixture(scope='function')
    def restricted_session(self, pytestconfig, request, session, dropped):
        """Returns session restricted with groups."""
        full_groups = session.groups
        available_groups = [group for group in session.groups
                            if group not in dropped]
        session.groups = available_groups

        def set_full_groups():
            session.groups = full_groups

        request.addfinalizer(set_full_groups)

        return session

    @pytest.fixture(scope='class',
                    params=utils.get_testcases("recovery.dc.testcases"),
                    ids=utils.get_testcases_names("recovery.dc.testcases"))
    def recovery(self, pytestconfig, request, session, nodes, indexes, dropped):
        """Returns a structure of recovery data."""
        recovery_result = super(TestRecoveryDC, self).recovery(pytestconfig,
                                                               request,
                                                               session,
                                                               nodes,
                                                               indexes,
                                                               dropped)
        return recovery_result

    def test_inconsistent_keys_inaccessibility(self, session, recovery, dropped):
        """Inconsistent keys inaccessibility test.

        Testing that after recovery operation keys, which were available in some specific
        groups, are not available in other groups.

        """
        for key in recovery["keys"]["inconsistent"]:
            for group in dropped:
                async_read = session.read_data_from_groups(key, [group])

                assert_that(calling(async_read.wait),
                            raises(elliptics.NotFoundError),
                            "Inconsistent key is accessible after recovery operation in node, "
                            "where it must be inaccessible.")
