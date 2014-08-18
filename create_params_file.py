#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
import os.path


def get_global_parameters_from_args(args):
    params = {}
    if args.packages_dir:
        path = os.path.expandvars(args.packages_dir)
        path = os.path.expanduser(path)
        params["packages_dir"] = os.path.abspath(path)
    if args.elliptics_version:
        params["elliptics_version"] = args.elliptics_version
    return params


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', dest="tests_params", required=True,
                        help="path to store test parameters.")
    parser.add_argument('--test', dest="tests", action="append", default=[],
                        help="parameters for a specific test.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--elliptics-version', dest="elliptics_version",
                       help="version of elliptics packages.")
    group.add_argument('--packages-dir', dest="packages_dir",
                       help="path to directory with elliptics packages to install.")
    args = parser.parse_args()

    #TODO: remove _global section
    params = {"_global": get_global_parameters_from_args(args)}
    for test in args.tests:
        test_params = json.loads(test)
        params.update(test_params)

    with open(args.tests_params, "w") as cfg:
        json.dump(params, cfg)
