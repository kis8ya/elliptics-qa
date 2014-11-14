"""Test cases for elliptics recovery-merge tests.

Functions from this module will be used to parameterize fixture for elliptics
recovery-merge tests. These functions return a structure of recovery data (with
these data we will check recovery operation). For more information about this
structure see `recovery_skeleton` in `recovery.utils.testrecovery` module.

"""

import random
import copy

import recovery.utils.keys
import recovery.utils.testrecovery
import recovery.merge.utils


def default_recovery(options, session, nodes, dropped_nodes, indexes):
    """Test case for recovery with no special options."""
    result = copy.deepcopy(recovery.utils.testrecovery.recovery_skeleton)

    keys = recovery.utils.keys.get_keys_for_merge(options, session, dropped_nodes, indexes)

    result["keys"]["consistent"] = keys["consistent"]
    result["keys"]["recovered"] = keys["inconsistent"]

    node = random.choice(nodes)
    result["cmd"] = ["dnet_recovery",
                     "--remote", "{}:{}:2".format(node.host, node.port),
                     "--groups", ','.join([str(g) for g in session.groups]),
                     "merge"]

    return result


def recovery_with_nprocess_option(options, session, nodes, dropped_nodes, indexes):
    """Test case for recovery with `--nprocess` option."""
    result = default_recovery(options, session, nodes, dropped_nodes, indexes)

    # adding --nprocess option to default recovery command
    result["cmd"].extend(["--nprocess", "3"])

    return result


def recovery_with_dump_file_option(options, session, nodes, dropped_nodes, indexes):
    """Test case for recovery with `--dump-file` option."""
    DUMP_FILE_PATH = "./dump_file"

    result = recovery.merge.utils.get_recovery_with_dump_file_option(options,
                                                                     session,
                                                                     nodes,
                                                                     dropped_nodes,
                                                                     indexes,
                                                                     DUMP_FILE_PATH)

    recovery.utils.keys.dump_keys_to_file(session, result["keys"]["recovered"], DUMP_FILE_PATH)

    return result


def recovery_with_dump_file_option_negative(options, session, nodes, dropped_nodes, indexes):
    """Negative test case for recovery with `--dump-file` option.

    In a dump file there are some keys which are not exist in elliptics cluster.

    """
    NOT_EXISTENT_KEYS_PERCENTAGE = 0.33
    DUMP_FILE_PATH = "./dump_file"

    result = recovery.merge.utils.get_recovery_with_dump_file_option(options,
                                                                     session,
                                                                     nodes,
                                                                     dropped_nodes,
                                                                     indexes,
                                                                     DUMP_FILE_PATH)

    not_existent_keys_number = options.inconsistent_files_number * NOT_EXISTENT_KEYS_PERCENTAGE
    not_existent_keys = recovery.utils.keys.get_not_existent_keys(session, not_existent_keys_number)
    recovery.utils.keys.dump_keys_to_file(session,
                                          not_existent_keys + result["keys"]["recovered"].keys(),
                                          DUMP_FILE_PATH)

    return result


def recovery_with_one_node_option(options, session, nodes, dropped_nodes, indexes):
    """Test case for recovery with `--one-node` option."""
    result = copy.deepcopy(recovery.utils.testrecovery.recovery_skeleton)

    keys = recovery.utils.keys.get_keys_for_merge(options, session, dropped_nodes, indexes)

    # Restrict elliptics cluster: disable all backends in specified nodes
    recovery.utils.testrecovery.disable_all_backends(session,
                                                     dropped_nodes,
                                                     options.backends_number)
    node = random.choice(dropped_nodes)

    result["keys"]["consistent"] = keys["consistent"]
    result["keys"]["recovered"], result["keys"]["inconsistent"] = \
        recovery.utils.keys.split_node_keys(session, keys["inconsistent"], node)

    result["recovery_indexes"] = False

    result["cmd"] = ["dnet_recovery",
                     "--one-node", "{}:{}:2".format(node.host, node.port),
                     "--groups", ','.join([str(g) for g in session.groups]),
                     "merge"]

    # Enable all disabled backends
    recovery.utils.testrecovery.enable_all_backends(session, dropped_nodes, options.backends_number)

    return result
