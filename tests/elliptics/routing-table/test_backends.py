"""Routing table tests for backends test cases."""

import pytest
import elliptics
import socket

from routing_table_utils import (AbstractTestRoutingTableEntries, backends_cases,
                                 routes_for_nodes_with_enabled_backends)


class TestAddingAtStart(AbstractTestRoutingTableEntries):
    """Test case for adding routing table entries at starting node."""

    @pytest.fixture(scope='class')
    def routing_entries(self, request, nodes, session):
        """Returns routing table entries for all enabled backends."""
        return routes_for_nodes_with_enabled_backends(nodes, session)


class TestEnableBackends(AbstractTestRoutingTableEntries):
    """Test case for adding routing table entries after enabling backends."""

    @pytest.fixture(scope='class',
                    params=backends_cases.values(),
                    ids=backends_cases.keys())
    def enable_backends(self, request, session, nodes):
        """Enables backends."""
        for node in nodes:
            address = elliptics.Address(node.host, node.port, socket.AF_INET)
            for backend_id in request.param(request.config.option.backends_number):
                session.enable_backend(address, backend_id).wait()

    @pytest.fixture(scope='class')
    def routing_entries(self, request, nodes, enable_backends, session):
        """Returns routing table entries for all enabled backends."""
        return routes_for_nodes_with_enabled_backends(nodes, session)


class TestDisableBackends(AbstractTestRoutingTableEntries):
    """Test case for adding routing table entries after disabling backends."""

    @pytest.fixture(scope='class',
                    params=backends_cases.values(),
                    ids=backends_cases.keys())
    def disable_backends(self, request, session, nodes):
        """Disables backends."""
        for node in nodes:
            address = elliptics.Address(node.host, node.port, socket.AF_INET)
            for backend_id in request.param(request.config.option.backends_number):
                session.disable_backend(address, backend_id).wait()

    @pytest.fixture(scope='class')
    def routing_entries(self, request, nodes, disable_backends, session):
        """Returns routing table entries for all enabled backends."""
        return routes_for_nodes_with_enabled_backends(nodes, session)
