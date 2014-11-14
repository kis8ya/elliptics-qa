import copy
import random

import recovery

def get_recovery_with_dump_file_option(options, session, nodes,
                                       dropped_nodes, indexes, dump_file_path):
    result = copy.deepcopy(recovery.utils.testrecovery.recovery_skeleton)

    keys = recovery.utils.keys.get_keys_for_merge(options, session, dropped_nodes, indexes)

    result["keys"]["consistent"] = keys["consistent"]
    result["keys"]["recovered"], result["keys"]["inconsistent"] = \
        recovery.utils.keys.split_keys_in_percentage(keys["inconsistent"],
                                                     options.inconsistent_files_percentage)

    result["recovery_indexes"] = False

    node = random.choice(nodes)

    result["cmd"] = ["dnet_recovery",
                     "--remote", "{}:{}:2".format(node.host, node.port),
                     "--groups", ','.join([str(g) for g in session.groups]),
                     "--dump-file", dump_file_path,
                     "merge"]
    return result
