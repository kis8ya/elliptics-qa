#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""This script prints elliptics version by specified branch from elliptics repo."""


import argparse
import requests
import sys


EXIT_OK = 0
EXIT_WRONGBRANCH = 1


if __name__ == "__main__":
    exitcode = EXIT_OK

    parser = argparse.ArgumentParser()
    parser.add_argument('branch')
    args = parser.parse_args()

    OWNER = "reverbrain"
    REPO = "elliptics"

    if args.branch.startswith('pull/'):
        # Get pull request number (pull/#NUMBER/merge)
        pr_number = args.branch.split('/')[1]
        url = "https://api.github.com/repos/{}/{}/pulls/{}".format(OWNER, REPO, pr_number)
        r = requests.get(url)
        pr_info = r.json()
        target_branch = pr_info["base"]["ref"]
    else:
        target_branch = args.branch

    if target_branch == "master":
        print("v2.26")
    elif target_branch == "v2.25":
        print("v2.25")
    else:
        exitcode = EXIT_WRONGBRANCH

    sys.exit(exitcode)
