#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import fnmatch
import ConfigParser
import pytest
import subprocess
import logging

import ansible_manager
import teamcity_messages

from config_template_renderer import get_cfg

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
        repo_dir = os.path.dirname(os.path.abspath(__file__))
        self.tests_dir = os.path.join(repo_dir, "tests")
        self.ansible_dir = os.path.join(repo_dir, "ansible")
        self.configs_dir = args.configs_dir

        self.logger = logging.getLogger('runner_logger')
        self.teamcity = args.teamcity

        self.instances_names = {'client': "{0}-client".format(args.instance_name),
                                'server': "{0}-server".format(args.instance_name)}
        self.tests = self.collect_tests(args.tags)

    def collect_tests(self, tags):
        """Collects tests' configs with given tags
        """
        tests = {}
        for root, dirs, filenames in os.walk(self.configs_dir):
            for filename in fnmatch.filter(filenames, 'test_*.cfg'):
                path = os.path.abspath(os.path.join(root, filename))
                cfg = get_cfg(path, self.instances_names)
                if set(cfg["tags"]).intersection(set(tags)):
                    # test config name format: "test_NAME.cfg"
                    test_name = os.path.splitext(filename)[0][5:]
                    tests[test_name] = cfg
        return tests

    def generate_pytest_cfg(self, test_config):
        """Generates pytest.ini with test options
        """
        pytest_config = ConfigParser.ConfigParser()
        pytest_config.add_section("pytest")
        pytest_config.set("pytest", "addopts", test_config["addopts"])

        self.logger.info("Test running options: {0}".format(test_config["addopts"]))
        with open(os.path.join(self.tests_dir, "pytest.ini"), "w") as config_file:
            pytest_config.write(config_file)

    def setup(self, test_name):
        test = self.tests[test_name]
        # Do prerequisite steps for a test
        ansible_manager.run_playbook(self.abspath(test["test_env_cfg"]["setup_playbook"]),
                                     self.get_inventory_path(test_name))

        # Check if it's a pytest test
        if "dir" in test:
            self.generate_pytest_cfg(test)

        cfg_info = "Test environment configuration:\n\tclients: {0}\n\tservers per group: {1}"
        self.logger.info(cfg_info.format(test["test_env_cfg"]["clients"]["count"],
                                         test["test_env_cfg"]["servers"]["count_per_group"]))

    def run_playbook_test(self, test_name):
        ansible_manager.run_playbook(self.abspath(self.tests[test_name]["playbook"]),
                                     self.get_inventory_path(test_name))

    def run_pytest_test(self, test_name):
        #TODO: remove this list of files
        # It can be replaced with copying all files from lib
        # or we can create separted directory for files which is needed for tests.
        files_to_sync = ["elliptics_testhelper.py", "utils.py", "logging_tests.py",
                         "logger.ini", "matchers.py"]
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--configs-dir', dest="configs_dir", required=True,
                        help="directory with tests' configs")
    parser.add_argument('--tag', action="append", dest="tags",
                        help="specifying which tests to run.")
    parser.add_argument('--instance-name', dest="instance_name", default="elliptics",
                        help="base name for the instances.")
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
        with teamcity_messages.block("LOGS: Links"):
            path = "/tmp/logs-archive"
            for f in os.listdir(path):
                print(qa_storage_upload(os.path.join(path, f)))
