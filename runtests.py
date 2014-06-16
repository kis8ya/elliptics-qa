#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import json
import sys
import glob
import ConfigParser
import pytest
import subprocess
import logging

import ansible_manager
import instances_manager
import teamcity_messages

# util functions
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

def setup_loggers(teamcity, verbose):
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)

    tc_logging_level = logging.INFO if teamcity else logging.ERROR
    tc_logger = logging.getLogger('teamcity_logger')
    tc_logger.setLevel(tc_logging_level)
    tc_logger.addHandler(handler)

    runner_logging_level = logging.INFO if verbose else logging.ERROR
    runner_logger = logging.getLogger('runner_logger')
    runner_logger.setLevel(runner_logging_level)
    runner_logger.addHandler(handler)

    conf_file = 'lib/logger.ini'
    parser = ConfigParser.ConfigParser()
    parser.read([conf_file])

    tests_logging_level = "ERROR" if teamcity else "INFO"
    parser.set('logger_testLogger', 'level', tests_logging_level)

    with open(conf_file, "w") as conf:
        parser.write(conf)
#END of util functions

class TestRunner(object):
    def __init__(self, args):
        self.repo_dir = os.path.dirname(os.path.abspath(__file__))
        self.tests_dir = os.path.join(self.repo_dir, "tests")
        self.ansible_dir = os.path.join(self.repo_dir, "ansible")
        self.packages_dir = args.packages_dir
        self.testsuite_params = json.loads(args.testsuite_params)
        self.tags = args.tag
        self.custom_instance_name = args.custom_instance_name

        self.logger = logging.getLogger('runner_logger')
        self.teamcity = args.teamcity

        self.tests = None
        self.instances_names = None
        self.instances_params = None

        self.prepare_base_environment()


    def collect_tests(self):
        """Collects information about tests to run
        """
        self.tests = {}
        # Collect all tests with specific tags
        tests_dirs = [os.path.join(self.tests_dir, s) for s in os.listdir(self.tests_dir)
                      if os.path.isdir(os.path.join(self.tests_dir, s))]

        for test_dir in tests_dirs:
            for cfg_file in glob.glob(os.path.join(test_dir, "test_*.cfg")):
                cfg = json.load(open(cfg_file))
                if set(cfg["tags"]).intersection(set(self.tags)):
                    # test config name format: "test_NAME.cfg"
                    test_name = os.path.splitext(os.path.basename(cfg_file))[0][5:]
                    self.tests[test_name] = cfg

    def collect_instances_params(self):
        """Returns information about clients and servers
        """

        image = "elliptics"
        instances_params = {"clients": {"count": 0, "flavor": None, "image": image},
                                 "servers": {"count": 0, "flavor": None, "image": image}}

        for test_cfg in self.tests.values():
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

    def prepare_ansible_test_files(self):
        """Prepares ansible inventory and vars files for the tests
        """
        # set global params for test suite
        global_params = self.testsuite_params.get("_global", {})
        ansible_manager.update_vars(vars_path=self._get_vars_path('test'),
                                    params=global_params)

        if global_params.get('config_format', '') == "stable":
            config_format = "conf"
        else:
            config_format = "json"
        params = {"elliptics_config": "templates/elliptics.{0}.j2".format(config_format)}
        ansible_manager.update_vars(vars_path=self._get_vars_path('test'),
                                    params=params)

        for name, cfg in self.tests.items():
            groups = ansible_manager._get_groups_names(name)
            inventory_path = self.get_inventory_path(name)

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

    def install_elliptics_packages(self):
        """Installs elliptics packages on all servers and clients
        """
        base_setup_playbook = "test-env-prepare"
        inventory_path = self.get_inventory_path(base_setup_playbook)
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

        ansible_manager.run_playbook(self.abspath(base_setup_playbook),
                                     inventory_path)

    @teamcity_messages.block("PREPARE TEST ENVIRONMENT")
    def prepare_base_environment(self):
        """ Prepares base test environment
        """
        self.collect_tests()

        if self.custom_instance_name:
            self.instances_names = {'client': "{0}-client".format(self.custom_instance_name),
                                    'server': "{0}-server".format(self.custom_instance_name)}
        else:
            self.instances_names = {'client': "elliptics-client",
                                    'server': "elliptics-server"}
        self.instances_params = self.collect_instances_params()
        instances_cfg = instances_manager.get_instances_cfg(self.instances_params,
                                                            self.instances_names)

        if not instances_manager.create(instances_cfg):
            raise RuntimeError("Not all nodes available")

        self.prepare_ansible_test_files()
        self.install_elliptics_packages()

    def generate_pytest_cfg(self, test_name):
        """Generates pytest.ini with test options
        """
        opts = self.tests[test_name]["addopts"].format(**self.tests[test_name]["params"])

        servers_per_group = self.tests[test_name]["test_env_cfg"]["servers"]["count_per_group"]
        groups_count = len(servers_per_group)
        servers_names = ansible_manager.get_host_names(self.instances_names['server'],
                                                       sum(servers_per_group))
        server_name = (x for x in servers_names)
        for g in xrange(groups_count):
            for i in xrange(servers_per_group[g]):
                opts += ' --node={0}.i.fog.yandex.net:1025:{1}'.format(next(server_name), g + 1)

        pytest_config = ConfigParser.ConfigParser()
        pytest_config.add_section("pytest")
        pytest_config.set("pytest", "addopts", opts)
        with open(os.path.join(self.tests_dir, "pytest.ini"), "w") as config_file:
            pytest_config.write(config_file)

        self.logger.info((open(os.path.join(self.tests_dir, "pytest.ini"))).read())

    def setup(self, test_name):
        test = self.tests[test_name]
        # Do prerequisite steps for a test
        ansible_manager.run_playbook(self.abspath(test["test_env_cfg"]["setup_playbook"]),
                                     self.get_inventory_path(test_name))

        # Check if it's a pytest test
        if test.get("dir"):
            self.generate_pytest_cfg(test_name)

        cfg_info = "Test environment configuration:\n\tclients: {0}\n\tservers per group: {1}"
        self.logger.info(cfg_info.format(test["test_env_cfg"]["clients"]["count"],
                                         test["test_env_cfg"]["servers"]["count_per_group"]))

    def run_playbook_test(self, test_name):
        ansible_manager.run_playbook(self.abspath(self.tests[test_name]["playbook"]),
                                     self.get_inventory_path(test_name))

    def run_pytest_test(self, test_name):
        files_to_sync = ["elliptics_testhelper.py", "utils.py", "logging_tests.py", "logger.ini"]
        rsyncdir_opts = "--rsyncdir tests/{0}/".format(self.tests[test_name]["dir"])
        for f in files_to_sync:
            rsyncdir_opts += " --rsyncdir lib/{0}".format(f)

        clients_names = ansible_manager.get_host_names(self.instances_names["client"],
                                                       self.tests[test_name]["test_env_cfg"]["clients"]["count"])
        for client_name in clients_names:
            if self.teamcity:
                opts = '--teamcity'
            else:
                opts = ''
            opts += ' -d --tx ssh="{0}.i.fog.yandex.net -l root -q" {1} tests/{2}/'

            opts = opts.format(client_name,
                               rsyncdir_opts,
                               self.tests[test_name]["dir"])
            self.logger.info(opts)
            pytest.main(opts)

    def run(self, test_name):
        if self.tests[test_name].get("playbook"):
            self.run_playbook_test(test_name)
        elif self.tests[test_name].get("dir"):
            self.run_pytest_test(test_name)
        else:
            self.logger.info("Can't specify running method for {0} test.\n".format(test_name))

    def teardown(self, test_name):
        # Do clean-up steps for a test
        ansible_manager.run_playbook(self.abspath(self.tests[test_name]["test_env_cfg"]["teardown_playbook"]),
                                     self.get_inventory_path(test_name))

    def run_tests(self):
        for test_name, test_cfg in self.tests.items():
            with teamcity_messages.block("TEST: {0}".format(test_name)):
                self.setup(test_name)
                self.run(test_name)
                self.teardown(test_name)

    def abspath(self, path):
        abs_path = os.path.join(self.ansible_dir, path)
        return abs_path

    def get_inventory_path(self, name):
        path = self.abspath("{0}.hosts".format(name))
        return path

    def _get_vars_path(self, name):
        path = self.abspath("group_vars/{0}.json".format(name))
        return path

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--testsuite-params', dest="testsuite_params", default="{}",
                        help="parameters which will override default parameters for specified test suite.")
    parser.add_argument('--packages-dir', dest="packages_dir",
                        help="path to directory with packages to install.")
    parser.add_argument('--tag', action="append", dest="tag",
                        help="specifying which tests to run.")
    parser.add_argument('--custom-instance-name', dest="custom_instance_name",
                        help="specifying custom base name for the instances.")

    parser.add_argument('--verbose', '-v', action="store_true", dest="verbose",
                        help="increase verbosity")
    parser.add_argument('--teamcity', action="store_true", dest="teamcity",
                        help="will format output with Teamcity messages.")
    args = parser.parse_args()

    setup_loggers(args.teamcity, args.verbose)

    testrunner = TestRunner(args)
    testrunner.run_tests()

    # collect logs
    with teamcity_messages.block("LOGS: Collecting logs"):
        ansible_manager.run_playbook(testrunner.abspath("collect-logs"),
                                     testrunner.get_inventory_path("test-env-prepare"))
        if args.teamcity:
            path = "/tmp/logs-archive"
            logs = []
            for f in os.listdir(path):
                logs.append(qa_storage_upload(os.path.join(path, f)))

    if args.teamcity:
        with teamcity_messages.block("LOGS: Links"):
            print('\n'.join(logs))
