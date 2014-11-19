"""Routing table acceptance tests."""

import pytest
import random
import time

from test_helper import network
from test_helper import utils
from test_helper.logging_tests import logger

from routing_table_utils import (get_routes_for_nodes, drop_half_nodes,
                                 AbstractTestRoutingTableEntries)


class TestAddingAtStart(AbstractTestRoutingTableEntries):
    """Test case for adding routing table entries at starting node."""

    @pytest.fixture(scope='class')
    def routing_entries(self, request, nodes):
        """Returns routing table entries.

        At starting node must have entries about all it remotes.

        """
        return get_routes_for_nodes(nodes, request.config.option.backends_number)


class TestRemovingAfterNodeDrop(AbstractTestRoutingTableEntries):
    """Test case for removing routing table entries after dropping nodes."""

    @pytest.fixture(scope='class')
    def dropped_nodes(self, request, nodes, session):
        """Returns list of dropped nodes.

        This fixture drops half nodes and returns a list of these nodes. After tests, for
        this test case, the fixture resumes all dropped nodes.

        """
        dropped_nodes_result = drop_half_nodes(nodes, session)

        def resume_nodes():
            for node in dropped_nodes_result:
                network.resume_node(node)

        request.addfinalizer(resume_nodes)

        return dropped_nodes_result

    @pytest.fixture(scope='class')
    def routing_entries(self, request, nodes, dropped_nodes):
        """Returns routing table entries.

        After dropping some nodes entries about these nodes must be removed and routing
        table must contain entries only about available nodes.

        """
        available_nodes = [node for node in nodes if node not in dropped_nodes]
        return get_routes_for_nodes(available_nodes, request.config.option.backends_number)


class TestAddingAfterNodeResume(AbstractTestRoutingTableEntries):
    """Test case for adding routing table entries after resuming nodes."""

    @pytest.fixture(scope='class')
    def resumed_nodes(self, request, nodes, session):
        """Returns list of resumed nodes.

        This fixture drops half node, then resume them and returns list of these nodes.

        """
        result_nodes = drop_half_nodes(nodes, session)

        for node in result_nodes:
            network.resume_node(node)

        wait_time = request.config.option.check_timeout + 1
        logger.info("Wait {} seconds for routing table update...".format(wait_time))
        time.sleep(wait_time)

        return result_nodes

    @pytest.fixture(scope='class')
    def routing_entries(self, request, nodes, resumed_nodes):
        """Returns routing table entries.

        After resuming nodes there must be entires about all nodes in routing table.

        """
        return get_routes_for_nodes(nodes, request.config.option.backends_number)


class TestAddingAtUpdate(AbstractTestRoutingTableEntries):
    """Test case for adding routing table entries at updating routing table."""

    @pytest.fixture(scope='class')
    def all_nodes(self, request):
        """Returns list of all nodes."""
        return utils.get_nodes_from_option(request.config.option.nodes)

    @pytest.fixture(scope='class')
    def nodes(self, all_nodes):
        """Returns list of client node's remotes."""
        return [random.choice(all_nodes)]

    @pytest.fixture(scope='class')
    def routing_entries(self, request, all_nodes):
        """Returns routing table entries.

        After updating routing table there must be entries about all nodes:

          * entries about client node's remotes;
          * entries about remotes of client node's remotes.

        """
        return get_routes_for_nodes(all_nodes, request.config.option.backends_number)
