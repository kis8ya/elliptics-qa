#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import requests
import json
import os.path

def get_distribution_branch(branch):
    """Returns distribution branch (stable/testing)."""
    if branch.startswith('pull/'):
        # get pull request number (pull/#NUMBER/merge)
        pr_number = branch.split('/')[1]
        url = "https://api.github.com/repos/reverbrain/elliptics/pulls/{0}".format(pr_number)
        r = requests.get(url)
        pr_info = r.json()
        distribution_branch = pr_info["base"]["ref"]
    else:
        distribution_branch = branch

    if distribution_branch == "master":
        return "testing"
    elif distribution_branch == "lts":
        return "stable"
    else:
        raise RuntimeError("Wrong branch was specified: {0}".format(branch))

def get_parameters_from_args(args):
    params = {}
    branch = get_distribution_branch(args.branch)
    if branch == "testing":
        params["elliptics_config"] = "templates/elliptics.json.j2"
    else:
        params["elliptics_config"] = "templates/elliptics.conf.j2"
    if args.packages_dir:
        path = os.path.expandvars(args.packages_dir)
        path = os.path.expanduser(path)
        params["packages_dir"] = os.path.abspath(path)
    if args.elliptics_version:
        params["elliptics_version"] = args.elliptics_version
    return params

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--tests-config', dest="tests_config", required=True,
                        help="path to the file with tests parameters wich will be updated.")
    parser.add_argument('--branch', dest="branch", default="master", required=True,
                        help="target branch for a pull request. It will specify what format to use" +
                        "for elliptics config.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--elliptics-version', dest="elliptics_version",
                       help="version of elliptics packages.")
    group.add_argument('--packages-dir', dest="packages_dir",
                       help="path to directory with elliptics packages to install.")
    args = parser.parse_args()

    #TODO: remove _global section - all parameters should be on the 1st level
    params = {"_global": get_parameters_from_args(args)}

    with open(args.tests_config, "w") as cfg:
        json.dump(params, cfg)
