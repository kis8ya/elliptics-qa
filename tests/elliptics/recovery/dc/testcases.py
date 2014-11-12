"""Test cases for elliptics recovery-dc tests.

Functions from this module will be used to parameterize fixture for elliptics
recovery-dc tests. These functions return a structure of recovery data (with
these data we will check recovery operation). For more information about this
structure see `recovery_skeleton` in `recovery.utils.testrecovery` module.

"""

import random
import copy

import recovery.utils.keys, recovery.utils.testrecovery


def default_recovery(options, session, nodes, dropped_groups, indexes):
    """Test case for recovery with no special options."""
    result = copy.deepcopy(recovery.utils.testrecovery.recovery_skeleton)

    keys = recovery.utils.keys.get_keys_for_dc(options, session, dropped_groups, indexes)

    result["keys"]["consistent"] = keys["consistent"]
    result["keys"]["recovered"] = keys["inconsistent"]

    node = random.choice(nodes)
    result["cmd"] = ["dnet_recovery",
                     "--remote", "{}:{}:2".format(node.host, node.port),
                     "--groups", ','.join([str(g) for g in session.groups]),
                     "dc"]

    return result


def recovery_with_nprocess_option(options, session, nodes, dropped_groups, indexes):
    """Test case for recovery with `--nprocess` option."""
    result = default_recovery(options, session, nodes, dropped_groups, indexes)

    # adding --nprocess option to default recovery command
    result["cmd"].extend(["--nprocess", "3"])

    return result


def recovery_with_dump_file_option(options, session, nodes, dropped_groups, indexes):
    """Test case for recovery with `--dump-file` option."""
    result = copy.deepcopy(recovery.utils.testrecovery.recovery_skeleton)

    keys = recovery.utils.keys.get_keys_for_dc(options, session, dropped_groups, indexes)

    result["keys"]["consistent"] = keys["consistent"]
    result["keys"]["recovered"], result["keys"]["inconsistent"] = \
        recovery.utils.keys.split_keys_in_percentage(keys["inconsistent"],
                                                     options.inconsistent_files_percentage)

    result["recovery_indexes"] = False

    dump_file_path = "./dump_file"
    recovery.utils.keys.dump_keys_to_file(session, result["keys"]["recovered"], dump_file_path)
    node = random.choice(nodes)

    result["cmd"] = ["dnet_recovery",
                     "--remote", "{}:{}:2".format(node.host, node.port),
                     "--groups", ','.join([str(g) for g in session.groups]),
                     "--dump-file", dump_file_path,
                     "dc"]
    return result


def recovery_with_one_node_option(options, session, nodes, dropped_groups, indexes):
    """Test case for recovery with `--one-node` option."""
    result = copy.deepcopy(recovery.utils.testrecovery.recovery_skeleton)

    keys = recovery.utils.keys.get_keys_for_dc(options, session, dropped_groups, indexes)

    # Choose node for `--one-node` from dropped groups
    available_nodes = [node for node in nodes
                       if node.group not in dropped_groups]
    node = random.choice(available_nodes)

    result["keys"]["consistent"] = keys["consistent"]
    result["keys"]["recovered"], result["keys"]["inconsistent"] = \
        recovery.utils.keys.split_node_keys(session, keys["inconsistent"], node)

    result["recovery_indexes"] = False

    result["cmd"] = ["dnet_recovery",
                     "--one-node", "{}:{}:2".format(node.host, node.port),
                     "--groups", ','.join([str(g) for g in session.groups]),
                     "dc"]

    return result
