"""Test cases for elliptics recovery dc tests.

Public functions from this module will be used for parameterizing fixture
for elliptics recovery dc tests.

They have a parameter **recovery** to get a skeleton object with information
about writen keys to elliptics cluster. These functions return object with
specified information about recovery with specific options. For more information
about **recovery** object see `recovery` fixture from `test_recovery_dc.py`.

"""

import random
import copy

import utils


def default_recovery(session, nodes, recovery):
    """Returns an object with specified information about recovery with no special options."""
    result = copy.deepcopy(recovery)
    node = random.choice(nodes)
    result["cmd"] = ["dnet_recovery",
                     "--remote", "{}:{}:2".format(node.host, node.port),
                     "--groups", ','.join([str(g) for g in session.groups]),
                     "dc"]
    result["keys"]["bad"].update(result["keys"]["broken"])
    result["keys"]["broken"].clear()
    return result


def _dump_keys_to_file(session, keys, file_path):
    """Writes id of given keys to a dump file."""
    ids = [str(session.transform(k)) for k in keys]
    with open(file_path, "w") as dump_file:
        dump_file.write("\n".join(ids))


def recovery_with_dump_file_option(session, nodes, recovery):
    """Returns a command to run `dnet_recovery` with `--dump-file` option."""
    result = copy.deepcopy(recovery)
    dump_file_path = "./dump_file"
    _dump_keys_to_file(session, recovery["keys"]["bad"], dump_file_path)
    node = random.choice(nodes)
    result["cmd"] = ["dnet_recovery",
                     "--remote", "{}:{}:2".format(node.host, node.port),
                     "--groups", ','.join([str(g) for g in session.groups]),
                     "--dump-file", dump_file_path,
                     "dc"]
    return result


def recovery_with_nprocess_option(session, nodes, recovery):
    """Returns a command to run `dnet_recovery` with `--nprocess` option."""
    result = copy.deepcopy(recovery)
    node = random.choice(nodes)
    result["cmd"] = ["dnet_recovery",
                     "--remote", "{}:{}:2".format(node.host, node.port),
                     "--groups", ','.join([str(g) for g in session.groups]),
                     "--nprocess", "3",
                     "dc"]
    result["keys"]["bad"].update(result["keys"]["broken"])
    result["keys"]["broken"].clear()
    return result


def recovery_with_one_node_option(session, nodes, recovery):
    """Returns a command to run `dnet_recovery` with `--one-node` option."""
    result = copy.deepcopy(recovery)
    available_nodes = [node for node in nodes
                       if node.group not in recovery["dropped_groups"]]
    node = random.choice(available_nodes)
    result["cmd"] = ["dnet_recovery",
                     "--one-node", "{}:{}:2".format(node.host, node.port),
                     "--groups", ','.join([str(g) for g in session.groups]),
                     "dc"]
    # Redistribute keys depend on chosen node
    ranges = utils.get_ranges_by_session(session, nodes)
    node_bad_keys = {}
    node_broken_keys = {}
    for key_type in ["bad", "broken"]:
        for key in result["keys"][key_type]:
            key_id = session.transform(key)
            host = repr(node)
            if key_id in ranges[host]:
                node_bad_keys[key] = result["keys"][key_type][key]
            else:
                node_broken_keys[key] = result["keys"][key_type][key]
    result["keys"]["bad"] = node_bad_keys
    result["keys"]["broken"] = node_broken_keys
    return result
