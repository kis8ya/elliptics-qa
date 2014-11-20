"""Elliptics recovery-merge tests.

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
    --backends-number=3 \
    tests/elliptics/recovery/merge/test_recovery_dc.py

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
    --backends-number=3 \
    tests/elliptics/recovery/merge/test_recovery_dc.py

"""

import pytest
import random
import elliptics
import socket

from hamcrest import assert_that, calling, raises, equal_to, is_not

from test_helper import utils
from test_helper.elliptics_testhelper import nodes

from recovery.utils.testrecovery import AbstractTestRecovery


class TestRecoveryMerge(AbstractTestRecovery):
    def assert_valid_keys(self, session, recovery, keys_type):
        """Checks specified keys.

        It checks that:
        1. keys are accessible.
        2. keys have correct data.
        
        """
        for key in recovery["keys"][keys_type]:
            async_read = session.read_data(key)

            assert_that(calling(async_read.wait),
                        is_not(raises(elliptics.NotFoundError)),
                        "{} key is not accessible after recovery operation in node, "
                        "where it must be accessible.".format(keys_type))

            data_hash = utils.get_sha1(async_read.get()[0].data)

            assert_that(data_hash, equal_to(key),
                        "{} key's data mismatch after recovery operation.".format(keys_type))


    def get_expected_keys(self, recovery, group, index, dropped_nodes):
        """Returns a list of all keys for the index."""
        expected_keys = [key for key, key_indexes in recovery["keys"]["consistent"].items()
                         if index in key_indexes]
        # add recovered keys if their indexes were recovered or we are checking indexes in group
        # which was available and had indexes before recovery operation
        if recovery["recovery_indexes"]:
            expected_keys.extend([key for key, key_indexes in recovery["keys"]["recovered"].items()
                                  if index in key_indexes])
        return expected_keys

    @pytest.fixture(scope='class')
    def dropped(self, nodes):
        """Returns a list of dropped nodes."""
        nodes_count = len(nodes)
        dropped_nodes_count = (nodes_count + 1) / 2
        dropped_nodes = random.sample(nodes, dropped_nodes_count)

        return dropped_nodes

    @pytest.fixture(scope='function')
    def restricted_session(self, pytestconfig, request, session, dropped):
        """Returns session for restricted elliptics cluster."""
        for node in dropped:
            address = elliptics.Address(node.host, node.port, socket.AF_INET)
            for backend_id in xrange(pytestconfig.option.backends_number):
                session.disable_backend(address, backend_id).wait()

        def enable_backends():
            for node in dropped:
                address = elliptics.Address(node.host, node.port, socket.AF_INET)
                for backend_id in xrange(pytestconfig.option.backends_number):
                    session.enable_backend(address, backend_id).wait()

        request.addfinalizer(enable_backends)

        return session

    @pytest.fixture(scope='class',
                    params=utils.get_testcases("recovery.merge.testcases"),
                    ids=utils.get_testcases_names("recovery.merge.testcases"))
    def recovery(self, pytestconfig, request, session, nodes, indexes, dropped):
        """Returns a structure of recovery data."""
        recovery_result = super(TestRecoveryMerge, self).recovery(pytestconfig,
                                                                  request,
                                                                  session,
                                                                  nodes,
                                                                  indexes,
                                                                  dropped)
        return recovery_result

    def test_inconsistent_keys_inaccessibility(self, session, recovery):
        """Inconsistent keys inaccessibility test.

        Testing that after recovery operation keys, which were available in some specific
        groups, are not available in other groups.

        """
        for key in recovery["keys"]["inconsistent"]:
            async_read = session.read_data(key)

            assert_that(calling(async_read.wait),
                        raises(elliptics.NotFoundError),
                        "Inconsistent key is accessible after recovery operation in node, "
                        "where it must be inaccessible.")
