"""Test cases for elliptics recovery dc tests.

Functions from this module will be used to parameterize fixture for
elliptics recovery dc tests. These functions return a structure of
recovery's data (with these data we will check recovery operation).
For more information about this structure see `recovery_skeleton` below.

"""

import random
import copy
import socket

import utils


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


def default_recovery(options, session, nodes, dropped_groups, indexes):
    """Test case for recovery with no special options."""
    result = copy.deepcopy(recovery_skeleton)

    keys = utils.get_keys(options, session, dropped_groups, indexes)

    result["keys"]["consistent"] = keys["consistent"]
    result["keys"]["recovered"] = keys["inconsistent"]

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
    inconsistent_keys_number = int(len(keys["inconsistent"]) * options.inconsistent_files_percentage)
    inconsistent_keys = dict(keys["inconsistent"].items()[:inconsistent_keys_number])
    recovered_keys = dict(keys["inconsistent"].items()[inconsistent_keys_number:])

    result["keys"]["consistent"] = keys["consistent"]
    result["keys"]["recovered"] = recovered_keys
    result["keys"]["inconsistent"] = inconsistent_keys

    result["recovery_indexes"] = False

    dump_file_path = "./dump_file"
    utils.dump_keys_to_file(session, result["keys"]["recovered"], dump_file_path)
    node = random.choice(nodes)
    result["cmd"] = ["dnet_recovery",
                     "--remote", "{}:{}:2".format(node.host, node.port),
                     "--groups", ','.join([str(g) for g in session.groups]),
                     "--dump-file", dump_file_path,
                     "dc"]
    return result


def recovery_with_nprocess_option(options, session, nodes, dropped_groups, indexes):
    """Test case for recovery with `--nprocess` option."""
    result = default_recovery(options, session, nodes, dropped_groups, indexes)

    # adding --nprocess option to default recovery command
    result["cmd"].extend(["--nprocess", "3"])

    return result


def recovery_with_one_node_option(options, session, nodes, dropped_groups, indexes):
    """Test case for recovery with `--one-node` option."""
    result = copy.deepcopy(recovery_skeleton)

    available_nodes = [node for node in nodes
                       if node.group not in dropped_groups]
    node = random.choice(available_nodes)

    keys = utils.get_keys(options, session, dropped_groups, indexes)
    # Split inconsistent keys to recovered and still inconsistent keys
    # depend on following condition: "is the key belong to chosen node?"
    node_address = socket.gethostbyname(node.host)
    node_recovered_keys = {}
    node_inconsistent_keys = {}
    for key, key_indexes in keys["inconsistent"].items():
        key_id = session.transform(key)
        key_node = session.lookup_address(key_id, node.group)
        if node_address == key_node.host and \
           node.port == key_node.port:
            node_recovered_keys[key] = key_indexes
        else:
            node_inconsistent_keys[key] = key_indexes

    result["keys"]["consistent"] = keys["consistent"]
    result["keys"]["recovered"] = node_recovered_keys
    result["keys"]["inconsistent"] = node_inconsistent_keys

    result["recovery_indexes"] = False

    result["cmd"] = ["dnet_recovery",
                     "--one-node", "{}:{}:2".format(node.host, node.port),
                     "--groups", ','.join([str(g) for g in session.groups]),
                     "dc"]

    return result
