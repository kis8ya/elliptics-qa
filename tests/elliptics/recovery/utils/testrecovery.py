"""Utils for recovery tests."""

import pytest
import os
import time
import subprocess
import threading
import signal
import socket
import elliptics

from abc import ABCMeta, abstractmethod
from hamcrest import assert_that, equal_to

from test_helper import utils
from test_helper.logging_tests import logger
from test_helper.matchers import hasitem

from recovery.utils.keys import INDEX_DATA_TEMPLATE


recovery_skeleton = {
    "cmd": None,
    "exitcode": None,
    # flag to check "will indexes be recovered or not?"
    "recovery_indexes": True,
    "keys": {
        # consistent keys are accessible from all groups
        "consistent": {},
        # recovered keys are keys which were accessible from several groups and were recovered
        "recovered": {},
        # inconsistent keys are keys which were accessible from several groups and were not recovered
        "inconsistent": {}
    }
}


class AbstractTestRecovery(object):
    """Base recovery tests."""
    __metaclass__ = ABCMeta

    @abstractmethod
    def assert_valid_keys(self, session, recovery, key_type):
        pass

    @abstractmethod
    def get_expected_keys(self, recovery, group, index, dropped):
        pass
    
    @pytest.fixture(scope='class')
    def session(self, request, nodes):
        """Returns elliptics session."""
        return utils.create_session(nodes,
                                    request.config.option.check_timeout,
                                    request.config.option.wait_timeout,
                                    request.node.name)

    @pytest.fixture(scope='class')
    def indexes(self):
        """Returns randomly generated indexes."""
        indexes_count = 5
        index_length = 20
        return [os.urandom(index_length) for _ in xrange(indexes_count)]

    @abstractmethod
    @pytest.fixture(scope='function')
    def restricted_session(self, pytestconfig, request, session, dropped):
        pass

    @abstractmethod
    @pytest.fixture(scope='class')
    def dropped(self):
        pass

    @abstractmethod
    @pytest.fixture(scope='class')
    def recovery(self, pytestconfig, request, session, nodes, indexes, dropped):
        recovery_result = request.param(pytestconfig.option, session, nodes, dropped, indexes)

        logger.info("Wait for indexes to synchronize...\n")
        time.sleep(pytestconfig.option.cache_sync_timeout)

        logger.info("{}\n".format(recovery_result["cmd"]))
        recovery_cmd = Command(recovery_result["cmd"])
        recovery_cmd.run(pytestconfig.option.recovery_timeout)
        recovery_result["exitcode"] = recovery_cmd.returncode
        recovery_result["timedout"] = recovery_cmd.timedout
        return recovery_result

    def test_exit_code(self, recovery):
        """Testing that `dnet_recovery` will be processed with exit status code = 0."""
        assert_that(recovery["exitcode"], equal_to(0),
                    "`dnet_recovery` exited with non-zero exit status code: {}\n"
                    "Running details: {}".format(recovery["exitcode"], recovery["cmd"]))

    def test_elapsed_time(self, recovery):
        """Testing that `dnet_recovery` will not take too much time."""
        assert_that(recovery["timedout"], equal_to(False),
                    "`dnet_recovery` took too long time.")

    def test_consistent_keys(self, session, recovery):
        """Consistent keys test.

        Testing that after recovery operation keys, which were availabe in all groups,
        are still available in all groups and have correct data.

        """
        self.assert_valid_keys(session, recovery, "consistent")

    def test_recovered_keys(self, session, recovery):
        """Recovered keys test.

        Testing that after recovery operation keys, which had to be recovered,
        are available in all groups and have correct data.

        """
        self.assert_valid_keys(session, recovery, "recovered")

    def test_inconsistent_keys(self, restricted_session, recovery):
        """Inconsistent keys test.

        Testing that after recovery operation keys, which were available in some specific
        groups, are still available in these groups and have correct data.

        """
        self.assert_valid_keys(restricted_session, recovery, "inconsistent")

    @abstractmethod
    def test_inconsistent_keys_inaccessibility(self, session, recovery):
        pass

    def test_indexes_searching(self, session, recovery, indexes, dropped):
        """Index searching test.

        Testing that after recovery operation searching by indexes, which were not
        available in some groups before recovery operation, will return all keys for each
        index in each group.

        """
        restricted_session = session.clone()
        for index in indexes:
            for group in session.groups:
                restricted_session.groups = [group]

                result_keys = restricted_session.find_all_indexes([index]).get()
                result_keys_ids = [key.id for key in result_keys]

                expected_keys = self.get_expected_keys(recovery, group, index, dropped)
                for expected_key in expected_keys:
                    expected_key_id = restricted_session.transform(expected_key)

                    assert_that(result_keys_ids, hasitem(expected_key_id),
                                'Expected key "{}" not found when was searching for index "{}" '
                                'in group {}'.format(expected_key, index, group))

    def test_key_index_data(self, session, recovery, indexes, dropped):
        """key-index data test.

        Testing that after recovery operation key-index data will be correct for all
        keys, which had to be recovered or were available before recovery operation.

        """
        restricted_session = session.clone()
        for index in indexes:
            for group in session.groups:
                restricted_session.groups = [group]

                expected_keys = self.get_expected_keys(recovery, group, index, dropped)
                for expected_key in expected_keys:
                    key_indexes = restricted_session.list_indexes(expected_key).get()
                    filtered_data = [index_entry.data for index_entry in key_indexes
                                     if index_entry.index == restricted_session.transform(index)]
                    key_index_data = filtered_data[0]
                    expected_index_data = INDEX_DATA_TEMPLATE.format(expected_key, index)

                    assert_that(key_index_data, equal_to(expected_index_data),
                                'Key-index (key = "{}", index = "{}") data mismatch in group {}'
                                .format(expected_key, index, group))


class Command(object):
    """Enables to run subprocess commands in a different thread with timeout."""
    def __init__(self, command):
        self.command = command
        self.returncode = None
        self.timedout = False
        self._process = None

    def run(self, timeout=None):
        def target():
            self._process = subprocess.Popen(self.command, preexec_fn=os.setpgrp)
            self._process.communicate()
            self.returncode = self._process.returncode

        thread = threading.Thread(target=target)
        thread.start()
        thread.join(timeout)
        if thread.is_alive():
            self.timedout = True
            os.killpg(self._process.pid, signal.SIGKILL)
            thread.join()
        if self.returncode == -signal.SIGABRT:
            os.killpg(self._process.pid, signal.SIGKILL)
        return self.returncode


def disable_all_backends(session, nodes, backends_number):
    """Disable all backends in specified nodes."""
    for node in nodes:
        address = elliptics.Address(node.host, node.port, socket.AF_INET)
        for backend_id in xrange(backends_number):
            session.disable_backend(address, backend_id).wait()


def enable_all_backends(session, nodes, backends_number):
    """Enable all backends in specified nodes."""
    for node in nodes:
        address = elliptics.Address(node.host, node.port, socket.AF_INET)
        for backend_id in xrange(backends_number):
            session.enable_backend(address, backend_id).wait()
