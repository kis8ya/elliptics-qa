"""Test cases for elliptics recovery dc tests.

Functions from this module will be used to parameterize fixture
for elliptics recovery dc tests. These functions return a structure
of recovery's data. For more information about this structure see
`recovery_skeleton` below.

"""

import random
import copy

import utils


recovery_skeleton = {
    "cmd": None,
    "exitcode": None,
    "keys": {
        "good": {},
        "bad": {},
        "broken": {}
    }
}


def default_recovery(options, session, nodes, dropped_groups, indexes):
    """Test case for recovery with no special options."""
    result = copy.deepcopy(recovery_skeleton)

    keys = utils.get_keys(options, session, dropped_groups, indexes)

    result["keys"]["good"] = keys["consistent"]
    result["keys"]["bad"] = keys["inconsistent"]

    node = random.choice(nodes)
    result["cmd"] = ["dnet_recovery",
                     "--remote", "{}:{}:2".format(node.host, node.port),
                     "--groups", ','.join([str(g) for g in session.groups]),
                     "dc"]

    return result


def recovery_with_dump_file_option(options, session, nodes, dropped_groups, indexes):
    """Test case for recovery with `--dump-file` option."""
    result = copy.deepcopy(recovery_skeleton)

    keys = utils.get_keys(options, session, dropped_groups, indexes)
    # Split inconsistent keys to "bad" and "broken" keys
    broken_keys_number = int(len(keys["inconsistent"]) * options.broken_files_percentage)
    broken_keys = dict(keys["inconsistent"].items()[:broken_keys_number])
    bad_keys = dict(keys["inconsistent"].items()[broken_keys_number:])
    # Remove indexes from bad keys - they will not be recovered
    for key in bad_keys:
        bad_keys[key] = set()

    result["keys"]["good"] = keys["consistent"]
    result["keys"]["bad"] = bad_keys
    result["keys"]["broken"] = broken_keys

    dump_file_path = "./dump_file"
    utils.dump_keys_to_file(session, result["keys"]["bad"], dump_file_path)
    node = random.choice(nodes)
    result["cmd"] = ["dnet_recovery",
                     "--remote", "{}:{}:2".format(node.host, node.port),
                     "--groups", ','.join([str(g) for g in session.groups]),
                     "--dump-file", dump_file_path,
                     "dc"]
    return result


def recovery_with_nprocess_option(options, session, nodes, dropped_groups, indexes):
    """Test case for recovery with `--nprocess` option."""
    result = copy.deepcopy(recovery_skeleton)

    keys = utils.get_keys(options, session, dropped_groups, indexes)

    result["keys"]["good"] = keys["consistent"]
    result["keys"]["bad"] = keys["inconsistent"]

    node = random.choice(nodes)
    result["cmd"] = ["dnet_recovery",
                     "--remote", "{}:{}:2".format(node.host, node.port),
                     "--groups", ','.join([str(g) for g in session.groups]),
                     "--nprocess", "3",
                     "dc"]

    return result


def recovery_with_one_node_option(options, session, nodes, dropped_groups, indexes):
    """Test case for recovery with `--one-node` option."""
    result = copy.deepcopy(recovery_skeleton)

    available_nodes = [node for node in nodes
                       if node.group not in dropped_groups]
    node = random.choice(available_nodes)

    keys = utils.get_keys(options, session, dropped_groups, indexes)
    # Split inconsistent keys to "bad" and "broken" keys
    # depend on following condition: "is the key belong to chosen node?"
    ranges = utils.get_ranges_by_session(session, nodes)
    host = repr(node)
    node_bad_keys = {}
    node_broken_keys = {}
    for key, key_indexes in keys["inconsistent"].items():
        key_id = session.transform(key)
        if key_id in ranges[host]:
            # Remove indexes from bad keys - they will not be recovered
            node_bad_keys[key] = set()
        else:
            node_broken_keys[key] = key_indexes

    result["keys"]["bad"] = node_bad_keys
    result["keys"]["broken"] = node_broken_keys

    result["cmd"] = ["dnet_recovery",
                     "--one-node", "{}:{}:2".format(node.host, node.port),
                     "--groups", ','.join([str(g) for g in session.groups]),
                     "dc"]

    return result
