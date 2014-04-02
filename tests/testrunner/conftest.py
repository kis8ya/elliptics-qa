# -*- coding: utf-8 -*-
import pytest
import os
import glob
import json
import re
import copy

import ansible_manager
import instances_manager

def pytest_addoption(parser):
    parser.addoption('--testsuite-params', type="string", action="store", dest="testsuite_params", default="{}")
    parser.addoption('--repo-dir', type="string", action="store", dest="repo_dir", default=None)
    parser.addoption('--packages-dir', type="string", action="store", dest="pkgs_dir", default=None)
    parser.addoption('--tag', type="string", action="append", dest="tag", default=None)

@pytest.mark.tryfirst
def pytest_configure(config):
    pytest.testrunner = TestRunner(config)
    config.pluginmanager.register(pytest.testrunner)

@pytest.mark.trylast
def pytest_unconfigure(config):
    config.pluginmanager.unregister(pytest.testrunner)

class TestRunner(object):
    def __init__(self, config):
        self.test_name = None

        self.repo_dir = config.option.repo_dir
        self.pkgs_dir = config.option.pkgs_dir
        self.tests_dir = os.path.join(self.repo_dir, "tests")
        self.ansible_dir = os.path.join(self.repo_dir, "ansible")
        self.testsuite_params = json.loads(config.option.testsuite_params, object_hook=self._decode_value)

        self.playbooks = {'base_setup': "test-env-prepare",
                          'elliptics_start': "elliptics-start",
                          'elliptics_stop': "elliptics-stop"}

        if self.testsuite_params.get("_branch"):
            self._branch = self.testsuite_params["_branch"]
        else:
            self._branch = "testing"

        self.instances_names = {'client': "elliptics-{0}-client".format(self._branch),
                                'server': "elliptics-{0}-server".format(self._branch)}

        self.prepare_base_environment(config.option)

    def run(self, name):
        playbook = self._abspath(self.tests[name]["playbook"])
        inventory = self._get_inventory_path(name)
        ansible_manager.run_playbook(playbook, inventory)
        
    # pytest hooks
    def pytest_runtest_setup(self, item):
        self.test_name = re.split("[\[\]]", item.name)[1]

        test = self.tests[self.test_name]
        # Prepare test environment for a specific test
        if test["test_env_cfg"].get("prepare_env"):
            ansible_manager.run_playbook(self._abspath(test["test_env_cfg"]["prepare_env"]),
                                         self._get_inventory_path(self.test_name))
        
        # Run elliptics process on all servers
        ansible_manager.run_playbook(self._abspath(self.playbooks['elliptics_start']),
                                     self._get_inventory_path(self.test_name))

    def pytest_runtest_teardown(self, item):
        ansible_manager.run_playbook(playbook=self._abspath(self.playbooks['elliptics_stop']),
                                     inventory=self._get_inventory_path(self.test_name))
    #/pytest hooks

    def prepare_base_environment(self, option):
        """ Prepares base test environment
        """
        self.collect_tests(option.tag)
        self.collect_instances_params()
        self.instances_cfg = instances_manager.get_instances_cfg(self.instances_params,
                                                                 self.instances_names)

        self.prepare_ansible_test_files()
        instances_manager.create(self.instances_cfg)
        self.install_elliptics_packages()

    def collect_tests(self, tags):
        """ Collects information about tests to run
        """
        self.tests = {}
        # Collect all tests with specific tags
        tests_dirs = [os.path.join(self.tests_dir, s) for s in os.listdir(self.tests_dir)
                      if os.path.isdir(os.path.join(self.tests_dir, s))]

        for test_dir in tests_dirs:
            for cfg_file in glob.glob(os.path.join(test_dir, "test_*.cfg")):
                cfg = json.load(open(cfg_file), object_hook=self._decode_value)
                if set(cfg["tags"]).intersection(set(tags)):
                    # test config name format: "test_NAME.cfg"
                    test_name = cfg_file.split('/')[-1][5:-4]
                    self.tests[test_name] = cfg

    def collect_instances_params(self):
        """ Collects information about clients and servers
        """
        if self._branch == "stable":
            image = "elliptics-lts"
        elif self._branch == "testing":
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

    def install_elliptics_packages(self):
        """ Installs elliptics packages on all servers and clients
        """
        inventory_path = self._get_inventory_path(self.playbooks['base_setup'])
        groups = ansible_manager._get_groups_names("setup")
        
        ansible_manager.generate_inventory_file(inventory_path=inventory_path,
                                                clients_count=self.instances_params["clients"]["count"],
                                                servers_per_group=[self.instances_params["servers"]["count"]],
                                                groups=groups,
                                                instances_names=self.instances_names)

        vars_path = self._get_vars_path('clients')
        ansible_manager.update_vars(vars_path=vars_path,
                                    params={"repo_dir": self.repo_dir})

        if self.pkgs_dir:
            ansible_manager.update_vars(vars_path=self._get_vars_path('test'),
                                        params={"packages_dir": self.pkgs_dir})

        ansible_manager.run_playbook(self._abspath(self.playbooks['base_setup']),
                                     inventory_path)

    def prepare_ansible_test_files(self):
        """ Prepares ansible inventory and vars files for the tests
        """
        # set global params for test suite
        if self.testsuite_params.get("_global"):
            ansible_manager.update_vars(vars_path=self._get_vars_path('test'),
                                        params=self.testsuite_params["_global"])

        for name, cfg in self.tests.items():
            groups = ansible_manager._get_groups_names(name)
            inventory_path = self._get_inventory_path(name)

            ansible_manager.generate_inventory_file(inventory_path=inventory_path,
                                                    clients_count=cfg["test_env_cfg"]["clients"]["count"],
                                                    servers_per_group=cfg["test_env_cfg"]["servers"]["count_per_group"],
                                                    groups=groups,
                                                    instances_names=self.instances_names)

            params = cfg["params"]
            if name in self.testsuite_params:
                params.update(self.testsuite_params[name])
            vars_path = self._get_vars_path(groups['test'])
            ansible_manager.set_vars(vars_path=vars_path, params=params)

    def _abspath(self, path):
        abs_path = os.path.join(self.ansible_dir, path)
        return abs_path

    def _get_inventory_path(self, name):
        path = self._abspath("{0}.hosts".format(name))
        return path
        
    def _get_vars_path(self, name):
        path = self._abspath("group_vars/{0}.yml".format(name))
        return path

    @staticmethod
    def _decode_list(data):
        res = []
        for i in data:
            res.append(TestRunner._decode_value(i))
        return res

    @staticmethod
    def _decode_object(data):
        res = {}
        for k, v in data.items():
            if isinstance(k, unicode):
                k = k.encode('utf-8')
            res[k] = TestRunner._decode_value(v)

        return res

    @staticmethod
    def _decode_value(data):
        res = data

        if isinstance(data, dict):
            res = TestRunner._decode_object(data)
        elif isinstance(data, list):
            res = TestRunner._decode_list(data)
        elif isinstance(data, unicode):
            res = data.encode('utf-8')

        return res
