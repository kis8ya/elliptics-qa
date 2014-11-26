import elliptics
import socket
import binascii
import random
import pytest
import copy

from abc import ABCMeta, abstractmethod
from hamcrest import assert_that, has_items, has_length, is_not, has_item

from test_helper import ssh
from test_helper import utils


_ID_LEN = 64
_ID_UPPER_BOUND = "ff" * _ID_LEN
_ID_LOWER_BOUND = "00" * _ID_LEN


DROPPED_NODES_CASES = {
    "ONE_NODE": lambda nodes: [random.choice(nodes)],
    "ALL_NODES": lambda nodes: nodes
    }


class AbstractTestRoutingTableEntries(object):
    """Base test class for routing table acceptance tests."""
    __metaclass__ = ABCMeta

    @pytest.fixture(scope='class')
    def session(self, request, nodes):
        """Returns elliptics session."""
        return utils.create_session(nodes,
                                    request.config.option.check_timeout,
                                    request.config.option.wait_timeout,
                                    request.node.name)

    @abstractmethod
    @pytest.fixture(scope='class')
    def routing_entries(self, nodes, request):
        pass

    def test_contains_necessary_entries(self, session, routing_entries):
        """Checks that client node's routing table contains all necessary entires.

        Client node's routing table must contain entries about it remote nodes and their
        remotes.

        """
        assert_that(session.routes, has_items(*routing_entries),
                    "client node doesn't have all necessary routing table entries")

    def test_boundary_entries(self, session, routing_entries):
        """Checks boundary entries in client node's routing table.

        If client node's routing table have any entry about any node, then it must
        contain boundary entries, which were inserted by client node itself. If client
        node's routing table has no entries about any node, than it must not have any
        boundary entry. These entries have following IDs:

          * 000000...000000;
          * ffffff...ffffff.

        """
        if len(routing_entries):
            upper_bound_entry = _get_routes_upper_bound(routing_entries)
            lower_bound_entry = _get_routes_lower_bound(routing_entries)

            assert_that(session.routes, has_items(upper_bound_entry, lower_bound_entry),
                        "client node doesn't have routing table entires with boundary IDs")
        else:
            actual_entries_ids = [str(entry.id) for entry in session.routes]

            for entry_id in [_ID_UPPER_BOUND, _ID_LOWER_BOUND]:
                assert_that(actual_entries_ids, is_not(has_item(entry_id)),
                            "client node has a routing table entry with boundary ID")

    def test_only_necessary_entries(self, session, routing_entries):
        """Checks that client node's routing table contains only necessary entries.

        Client node's routing table must have only following entries:

          * entries about it remotes;
          * entries about remotes of client node's remotes;
          * entries with boundary IDs.

        """
        entries_differance = [entry for entry in session.routes
                              if entry not in routing_entries and
                                 str(entry.id) != _ID_UPPER_BOUND and
                                 str(entry.id) != _ID_LOWER_BOUND]

        assert_that(entries_differance, has_length(0),
                    "client node has some extra routing table entries")


def _routing_entry_key(routing_entry):
    """Key function for sorting `elliptics.Route` objects."""
    return routing_entry.id


def _get_routes_upper_bound(routing_entries):
    """Returns routes entry with upper bound ID (ffffff...ffffff)."""
    upper_routing_entry = max(routing_entries, key=_routing_entry_key)
    upper_bound = copy.deepcopy(upper_routing_entry)
    upper_bound.id = elliptics.Id.from_hex(_ID_UPPER_BOUND)
    return upper_bound


def _get_routes_lower_bound(routing_entries):
    """Returns routes entry with lower bound ID (000000...000000)."""
    lower_routing_entry = min(routing_entries, key=_routing_entry_key)
    if str(lower_routing_entry.id) == _ID_LOWER_BOUND:
        lower_bound = copy.deepcopy(lower_routing_entry)
    else:
        # If ID != 000000...000000, then interval [000000...00000; MIN_ID) must belong
        # to node-backend with maximum ID
        upper_routing_entry = max(routing_entries, key=_routing_entry_key)
        lower_bound = copy.deepcopy(upper_routing_entry)
    lower_bound.id = elliptics.Id.from_hex(_ID_LOWER_BOUND)
    return lower_bound


def _str_to_ids(ids_string):
    """Converts string contant of ids file to `elliptics.Id` objects."""
    # Split all elliptics id
    ids_str = [ids_string[i:i+_ID_LEN] for i in xrange(0, len(ids_string), _ID_LEN)]
    # Get actual elliptics.Id objects
    ids_hex = [binascii.hexlify(id_str) for id_str in ids_str]
    ids = [elliptics.Id.from_hex(id_hex) for id_hex in ids_hex]
    return ids


def _get_routes_from_ids(node, backends_number, history_path="/mnt/elliptics/history"):
    """Returns routing table entries for specified node."""
    sshclient = ssh.get_sshclient(node.host)
    sftp = sshclient.open_sftp()
    address = elliptics.Address(node.host, node.port, socket.AF_INET)

    routes = []
    for backend_id in xrange(backends_number):
        # Get content of ids file
        ids_path = "{}/{}/ids".format(history_path, backend_id)
        ids_file = sftp.open(ids_path)
        ids = _str_to_ids(ids_file.read())

        backend_routes = [elliptics.Route(elliptics_id, address, backend_id)
                          for elliptics_id in ids]

        routes.extend(backend_routes)

    return routes


def get_routes_for_nodes(nodes, backends_number):
    """Returns routing table entries for all specified nodes."""
    routing_entries = [routing_entry
                       for node in nodes
                       for routing_entry in _get_routes_from_ids(node, backends_number)]
    return routing_entries
