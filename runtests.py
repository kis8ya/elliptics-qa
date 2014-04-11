#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import json
import requests
import sys
import glob
import ConfigParser
import pytest

import ansible_manager
import instances_manager
import openstack

# util functions
def _abspath(path):
    abs_path = os.path.join(ansible_dir, path)
    return abs_path

def _get_inventory_path(name):
    path = _abspath("{0}.hosts".format(name))
    return path

def _get_vars_path(name):
    path = _abspath("group_vars/{0}.yml".format(name))
    return path

def _decode_list(data):
    res = []
    for i in data:
        res.append(_decode_value(i))
    return res

def _decode_object(data):
    res = {}
    for k, v in data.items():
        if isinstance(k, unicode):
            k = k.encode('utf-8')
        res[k] = _decode_value(v)

    return res

def _decode_value(data):
    res = data

    if isinstance(data, dict):
        res = _decode_object(data)
    elif isinstance(data, list):
        res = _decode_list(data)
    elif isinstance(data, unicode):
        res = data.encode('utf-8')

    return res
#END of util functions

def get_target_branch():
    """Checks and returns target branch (master/lts)
    """
    if args.branch.startswith('pull/'):
        # get pull request number (pull/#NUMBER/merge)
        pr_number = args.branch.split('/')[1]
        url = "https://api.github.com/repos/reverbrain/elliptics/pulls/{0}".format(pr_number)
        r = requests.get(url)
        pr_info = r.json(object_hook=_decode_value)
        branch = pr_info["base"]["ref"]
    else:
        branch = args.branch

    if branch == "master":
        branch = "testing"
    elif branch == "lts":
        branch = "stable"
    else:
        sys.exit("Wrong branch was specified: {0}".format(branch))

    return branch

def collect_instances_params(branch):
    """Returns information about clients and servers
    """
    if branch == "stable":
        image = "elliptics-lts"
    elif branch == "testing":
        image = "elliptics"

    instances_params = {"clients": {"count": 0, "flavor": None, "image": image},
                        "servers": {"count": 0, "flavor": None, "image": image}}

    for test_cfg in tests.values():
        test_env = test_cfg["test_env_cfg"]
        for instance_type in ["clients", "servers"]:
            instances_params[instance_type]["flavor"] = max(instances_params[instance_type]["flavor"],
                                                            test_env[instance_type]["flavor"],
                                                            key=instances_manager._flavors_order)
        instances_params["clients"]["count"] = max(instances_params["clients"]["count"],
                                                   test_env["clients"]["count"])
        instances_params["servers"]["count"] = max(instances_params["servers"]["count"],
                                                   sum(test_env["servers"]["count_per_group"]))

    return instances_params

def prepare_ansible_test_files(branch):
    """Prepares ansible inventory and vars files for the tests
    """
    # set global params for test suite
    if testsuite_params.get("_global"):
        ansible_manager.update_vars(vars_path=_get_vars_path('test'),
                                    params=testsuite_params["_global"])

    if branch == "testing":
        config_format = "json"
    elif branch == "stable":
        config_format = "conf"
    params = {"elliptics_config": "templates/elliptics.{0}.j2".format(config_format)}
    ansible_manager.update_vars(vars_path=_get_vars_path('test'),
                                params=params)

    for name, cfg in tests.items():
        groups = ansible_manager._get_groups_names(name)
        inventory_path = _get_inventory_path(name)

        ansible_manager.generate_inventory_file(inventory_path=inventory_path,
                                                clients_count=cfg["test_env_cfg"]["clients"]["count"],
                                                servers_per_group=cfg["test_env_cfg"]["servers"]["count_per_group"],
                                                groups=groups,
                                                instances_names=instances_names)

        params = cfg["params"]
        if name in testsuite_params:
            params.update(testsuite_params[name])
        vars_path = _get_vars_path(groups['test'])
        ansible_manager.set_vars(vars_path=vars_path, params=params)

def install_elliptics_packages(instances_params):
    """Installs elliptics packages on all servers and clients
    """
    base_setup_playbook = "test-env-prepare"
    inventory_path = _get_inventory_path(base_setup_playbook)
    groups = ansible_manager._get_groups_names("setup")

    ansible_manager.generate_inventory_file(inventory_path=inventory_path,
                                            clients_count=instances_params["clients"]["count"],
                                            servers_per_group=[instances_params["servers"]["count"]],
                                            groups=groups,
                                            instances_names=instances_names)

    vars_path = _get_vars_path('clients')
    ansible_manager.update_vars(vars_path=vars_path,
                                params={"repo_dir": repo_dir})

    if packages_dir:
        ansible_manager.update_vars(vars_path=_get_vars_path('test'),
                                    params={"packages_dir": packages_dir})

    ansible_manager.run_playbook(_abspath(base_setup_playbook),
                                 inventory_path)

def prepare_base_environment(branch):
    """ Prepares base test environment
    """
    collect_tests(args.tag)

    global instances_names
    instances_names = {'client': "elliptics-{0}-client".format(branch),
                       'server': "elliptics-{0}-server".format(branch)}
    instances_params = collect_instances_params(branch)
    instances_cfg = instances_manager.get_instances_cfg(instances_params,
                                                        instances_names)

    prepare_ansible_test_files(branch)
    instances_manager.create(instances_cfg)
    install_elliptics_packages(instances_params)

def collect_tests(tags):
    """Collects information about tests to run
    """
    global tests
    tests = {}
    # Collect all tests with specific tags
    tests_dirs = [os.path.join(tests_dir, s) for s in os.listdir(tests_dir)
                  if os.path.isdir(os.path.join(tests_dir, s))]

    for test_dir in tests_dirs:
        for cfg_file in glob.glob(os.path.join(test_dir, "test_*.cfg")):
            cfg = json.load(open(cfg_file), object_hook=_decode_value)
            if set(cfg["tags"]).intersection(set(tags)):
                # test config name format: "test_NAME.cfg"
                test_name = os.path.splitext(os.path.basename(cfg_file))[0][5:]
                tests[test_name] = cfg

def run(name):
    opts = "-v -d --teamcity --tx ssh=root@{0}.i.fog.yandex.net --rsyncdir th/elliptics_testhelper.py --rsyncdir th/utils.py --rsyncdir tests/{1}/ tests/{1}/"
    opts = opts.format(instances_names['client'], tests[name]["dir"])
    print("running py.test")
    print(opts)
    pytest.main(opts)

    # playbook = _abspath(tests[name]["playbook"])
    # inventory = _get_inventory_path(name)
    # ansible_manager.run_playbook(playbook, inventory)

def setup(test_name):
    test = tests[test_name]
    # Prepare test environment for a specific test
    if test["test_env_cfg"].get("prepare_env"):
        ansible_manager.run_playbook(_abspath(test["test_env_cfg"]["prepare_env"]),
                                     _get_inventory_path(test_name))

    # Run elliptics process on all servers
    ansible_manager.run_playbook(_abspath("elliptics-start"),
                                 _get_inventory_path(test_name))

    opts = tests[test_name]["addopts"].format(**tests[test_name]["params"])

    servers_per_group = tests[test_name]["test_env_cfg"]["servers"]["count_per_group"]
    groups_count = len(servers_per_group)
    config = {"name": instances_names['server'],
              "max_count": sum(servers_per_group)}
    servers_names = openstack.utils.get_instances_names_from_conf(config)
    server_name = (x for x in servers_names)
    for g in xrange(groups_count):
        for i in xrange(servers_per_group[g]):
            opts += ' --node={0}.i.fog.yandex.net:1025:{1}'.format(next(server_name), g + 1)

    pytest_config = ConfigParser.ConfigParser()
    pytest_config.add_section("pytest")
    pytest_config.set("pytest", "addopts", opts)
    with open(os.path.join(tests_dir, "pytest.ini"), "w") as config_file:
        pytest_config.write(config_file)
    print(open(os.path.join(tests_dir, "pytest.ini")).read())

def teardown():
    ansible_manager.run_playbook(playbook=_abspath("elliptics-stop"),
                                 inventory=_get_inventory_path(test_name))

parser = argparse.ArgumentParser()
parser.add_argument('--branch', dest="branch", default="master")
parser.add_argument('--testsuite-params', dest="testsuite_params", default="{}")
parser.add_argument('--packages-dir', dest="packages_dir")
parser.add_argument('--tag', action="append", dest="tag")
args = parser.parse_args()

repo_dir = os.path.dirname(os.path.abspath(__file__))
tests_dir = os.path.join(repo_dir, "tests")
ansible_dir = os.path.join(repo_dir, "ansible")
packages_dir = args.packages_dir
testsuite_params = json.loads(args.testsuite_params, object_hook=_decode_value)

tests = None
instances_names = None

if __name__ == "__main__":
    branch = get_target_branch()

    prepare_base_environment(branch)

    # collect_tests(args.tag)
    # global instances_names
    # instances_names = {'client': "elliptics-{0}-client".format(branch),
    #                    'server': "elliptics-{0}-server".format(branch)}
    for test_name, test_cfg in tests.items():
        setup(test_name)
        run(test_name)
        teardown()
