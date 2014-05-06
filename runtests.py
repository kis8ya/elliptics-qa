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
import subprocess

import ansible_manager
import instances_manager
import openstack
import teamcity_messages

# util functions
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

def qa_storage_upload(file_path):
    build_name = os.environ['TEAMCITY_BUILDCONF_NAME']
    build_name = build_name.replace(' ', '_')
    build_number = os.environ['BUILD_NUMBER']
    file_name = os.path.basename(file_path)
    url = 'http://qa-storage.yandex-team.ru/upload/elliptics-testing/{build_name}/{build_number}/{file_name}'
    url = url.format(build_name=build_name, build_number=build_number, file_name=file_name)

    cmd = ["curl", url, "--data-binary", "@" + file_path]
    subprocess.call(cmd)

    url = url.replace("/upload/", "/get/")

    return url
#END of util functions

class TestRunner(object):
    def __init__(self, args):
        self.repo_dir = os.path.dirname(os.path.abspath(__file__))
        self.tests_dir = os.path.join(self.repo_dir, "tests")
        self.ansible_dir = os.path.join(self.repo_dir, "ansible")
        self.packages_dir = args.packages_dir
        self.testsuite_params = json.loads(args.testsuite_params, object_hook=_decode_value)
        self.tags = args.tag

        self.verbose_output = args.verbose

        self.tests = None
        self.instances_names = None
        self.instances_params = None

        self.collect_target_branch(args.branch)
        self.prepare_base_environment()

    def collect_target_branch(self, branch):
        """Checks (and stores) target branch (master/lts)
        """
        if branch.startswith('pull/'):
            # get pull request number (pull/#NUMBER/merge)
            pr_number = branch.split('/')[1]
            url = "https://api.github.com/repos/reverbrain/elliptics/pulls/{0}".format(pr_number)
            r = requests.get(url)
            pr_info = r.json(object_hook=_decode_value)
            self.branch = pr_info["base"]["ref"]
        else:
            self.branch = branch

        if branch == "master":
            self.branch = "testing"
        elif branch == "lts":
            self.branch = "stable"
        else:
            raise RuntimeError("Wrong branch was specified: {0}".format(branch))

    def collect_tests(self):
        """Collects information about tests to run
        """
        self.tests = {}
        # Collect all tests with specific tags
        tests_dirs = [os.path.join(self.tests_dir, s) for s in os.listdir(self.tests_dir)
                      if os.path.isdir(os.path.join(self.tests_dir, s))]

        for test_dir in tests_dirs:
            for cfg_file in glob.glob(os.path.join(test_dir, "test_*.cfg")):
                cfg = json.load(open(cfg_file), object_hook=_decode_value)
                if set(cfg["tags"]).intersection(set(self.tags)):
                    # test config name format: "test_NAME.cfg"
                    test_name = os.path.splitext(os.path.basename(cfg_file))[0][5:]
                    self.tests[test_name] = cfg

    def collect_instances_params(self):
        """Returns information about clients and servers
        """
        if self.branch == "stable":
            image = "elliptics-lts"
        else:
            image = "elliptics"

        self.instances_params = {"clients": {"count": 0, "flavor": None, "image": image},
                                 "servers": {"count": 0, "flavor": None, "image": image}}

        for test_cfg in self.tests.values():
            test_env = test_cfg["test_env_cfg"]
            for instance_type in ["clients", "servers"]:
                self.instances_params[instance_type]["flavor"] = max(self.instances_params[instance_type]["flavor"],
                                                                     test_env[instance_type]["flavor"],
                                                                     key=instances_manager._flavors_order)
            self.instances_params["clients"]["count"] = max(self.instances_params["clients"]["count"],
                                                            test_env["clients"]["count"])
            self.instances_params["servers"]["count"] = max(self.instances_params["servers"]["count"],
                                                            sum(test_env["servers"]["count_per_group"]))

    def prepare_ansible_test_files(self):
        """Prepares ansible inventory and vars files for the tests
        """
        # set global params for test suite
        if self.testsuite_params.get("_global"):
            ansible_manager.update_vars(vars_path=self._get_vars_path('test'),
                                        params=self.testsuite_params["_global"])

        if self.branch == "stable":
            config_format = "conf"
        else:
            config_format = "json"
        params = {"elliptics_config": "templates/elliptics.{0}.j2".format(config_format)}
        ansible_manager.update_vars(vars_path=self._get_vars_path('test'),
                                    params=params)

        for name, cfg in self.tests.items():
            groups = ansible_manager._get_groups_names(name)
            inventory_path = self._get_inventory_path(name)

            ansible_manager.generate_inventory_file(inventory_path=inventory_path,
                                                    clients_count=cfg["test_env_cfg"]["clients"]["count"],
                                                    servers_per_group=cfg["test_env_cfg"]["servers"]["count_per_group"],
                                                    groups=groups,
                                                    instances_names=self.instances_names)

    def install_elliptics_packages(self):
        """Installs elliptics packages on all servers and clients
        """
        base_setup_playbook = "test-env-prepare"
        inventory_path = self._get_inventory_path(base_setup_playbook)
        groups = ansible_manager._get_groups_names("setup")

        ansible_manager.generate_inventory_file(inventory_path=inventory_path,
                                                clients_count=self.instances_params["clients"]["count"],
                                                servers_per_group=[self.instances_params["servers"]["count"]],
                                                groups=groups,
                                                instances_names=self.instances_names)

        vars_path = self._get_vars_path('clients')
        ansible_manager.update_vars(vars_path=vars_path,
                                    params={"repo_dir": self.repo_dir})

        if self.packages_dir:
            ansible_manager.update_vars(vars_path=self._get_vars_path('test'),
                                        params={"packages_dir": self.packages_dir})

        ansible_manager.run_playbook(self._abspath(base_setup_playbook),
                                     inventory_path)

    def prepare_base_environment(self):
        """ Prepares base test environment
        """
        tc_block = "PREPARE TEST ENVIRONMENT"
        teamcity_messages.start_block(tc_block)

        self.collect_tests()

        self.instances_names = {'client': "elliptics-{0}-client".format(self.branch),
                                'server': "elliptics-{0}-server".format(self.branch)}
        self.collect_instances_params()
        instances_cfg = instances_manager.get_instances_cfg(self.instances_params,
                                                            self.instances_names)

        self.prepare_ansible_test_files()
        instances_manager.create(instances_cfg)
        self.install_elliptics_packages()

        teamcity_messages.end_block(tc_block)

    def generate_pytest_cfg(self, test_name):
        """Generates pytest.ini with test options
        """
        opts = self.tests[test_name]["addopts"].format(**self.tests[test_name]["params"])

        servers_per_group = self.tests[test_name]["test_env_cfg"]["servers"]["count_per_group"]
        groups_count = len(servers_per_group)
        config = {"name": self.instances_names['server'],
                  "max_count": sum(servers_per_group)}
        servers_names = openstack.utils.get_instances_names_from_conf(config)
        server_name = (x for x in servers_names)
        for g in xrange(groups_count):
            for i in xrange(servers_per_group[g]):
                opts += ' --node={0}.i.fog.yandex.net:1025:{1}'.format(next(server_name), g + 1)

        pytest_config = ConfigParser.ConfigParser()
        pytest_config.add_section("pytest")
        pytest_config.set("pytest", "addopts", opts)
        with open(os.path.join(self.tests_dir, "pytest.ini"), "w") as config_file:
            pytest_config.write(config_file)

        if self.verbose_output:
            print((open(os.path.join(self.tests_dir, "pytest.ini"))).read())

    def setup(self, test_name):
        test = self.tests[test_name]
        # Prepare test environment for a specific test
        if test["test_env_cfg"].get("prepare_env"):
            ansible_manager.run_playbook(self._abspath(test["test_env_cfg"]["prepare_env"]),
                                         self._get_inventory_path(test_name))

        # Run elliptics process on all servers
        ansible_manager.run_playbook(self._abspath("elliptics-start"),
                                     self._get_inventory_path(test_name))

        self.generate_pytest_cfg(test_name)

    def run(self, test_name):
        opts = '-d --teamcity --tx ssh="{0}.i.fog.yandex.net -l root -q" --rsyncdir lib/elliptics_testhelper.py --rsyncdir lib/utils.py --rsyncdir tests/{1}/ tests/{1}/'
        opts = opts.format(self.instances_names['client'], self.tests[test_name]["dir"])
        if self.verbose_output:
            print(opts)
        pytest.main(opts)

    def teardown(self, test_name):
        ansible_manager.run_playbook(playbook=self._abspath("elliptics-stop"),
                                     inventory=self._get_inventory_path(test_name))

    def run_tests(self):
        for test_name, test_cfg in self.tests.items():
            tc_block = "TEST: {0}".format(test_name)
            teamcity_messages.start_block(tc_block)

            self.setup(test_name)
            self.run(test_name)
            self.teardown(test_name)

            teamcity_messages.end_block(tc_block)

    def _abspath(self, path):
        abs_path = os.path.join(self.ansible_dir, path)
        return abs_path

    def _get_inventory_path(self, name):
        path = self._abspath("{0}.hosts".format(name))
        return path

    def _get_vars_path(self, name):
        path = self._abspath("group_vars/{0}.yml".format(name))
        return path

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--branch', dest="branch", default="master")
    parser.add_argument('--testsuite-params', dest="testsuite_params", default="{}")
    parser.add_argument('--packages-dir', dest="packages_dir")
    parser.add_argument('--tag', action="append", dest="tag")

    parser.add_argument('--verbose', '-v', action="store_true", dest="verbose")
    args = parser.parse_args()

    testrunner = TestRunner(args)

    testrunner.run_tests()

    # collect logs
    tc_block = "LOGS: Collecting logs"
    teamcity_messages.start_block(tc_block)
    ansible_manager.run_playbook(_abspath("collect-logs"),
                                 _get_inventory_path("test-env-prepare"))
    path = "/tmp/logs-archive"
    logs = []
    for f in os.listdir(path):
        logs.append(qa_storage_upload(os.path.join(path, f)))
    teamcity_messages.end_block(tc_block)

    tc_block = "LOGS: Links"
    teamcity_messages.start_block(tc_block)
    print('\n'.join(logs))
    teamcity_messages.end_block(tc_block)
