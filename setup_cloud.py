#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import json
import sys
import fnmatch
import ConfigParser
import logging

import ansible_manager
import instances_manager
import teamcity_messages

from config_template_renderer import get_cfg

def setup_loggers(teamcity, verbose):
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)

    tc_logging_level = logging.INFO if teamcity else logging.ERROR
    tc_logger = logging.getLogger('teamcity_logger')
    tc_logger.setLevel(tc_logging_level)
    tc_logger.addHandler(handler)

    conf_file = 'lib/logger.ini'
    parser = ConfigParser.ConfigParser()
    parser.read([conf_file])

    tests_logging_level = "ERROR" if teamcity else "INFO"
    parser.set('logger_testLogger', 'level', tests_logging_level)

    with open(conf_file, "w") as conf:
        parser.write(conf)
#END of util functions

class Cloud(object):
    def __init__(self, args):
        repo_dir = os.path.dirname(os.path.abspath(__file__))
        self.ansible_dir = os.path.join(repo_dir, "ansible")
        self.configs_dir = args.configs_dir
        if args.testsuite_params:
            with open(args.testsuite_params, 'r') as f:
                self.testsuite_params = json.load(f)
        else:
            self.testsuite_params = {}

        self.instances_names = {'client': "{0}-client".format(args.instance_name),
                                'server': "{0}-server".format(args.instance_name)}
        self.tests = self.collect_tests(args.tags)
        self.instances_params = self.collect_instances_params()

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

    def collect_instances_params(self):
        """Returns information about clients and servers
        """

        instances_params = {'clients': {'count': 0, 'flavor': None, 'image': 'elliptics'},
                            'servers': {'count': 0, 'flavor': None, 'image': 'elliptics'}}

        clients_params = instances_params['clients']
        tests_params = [test_cfg['test_env_cfg']['clients'] for test_cfg in self.tests.values()]

        clients_params['flavor'] = max((test_params['flavor'] for test_params in tests_params),
                                       key=instances_manager._flavors_order)
        clients_params['count'] = max(test_params['count'] for test_params in tests_params)

        servers_params = instances_params['servers']
        tests_params = [test_cfg['test_env_cfg']['servers'] for test_cfg in self.tests.values()]

        servers_params['flavor'] = max((test_params['flavor'] for test_params in tests_params),
                                       key=instances_manager._flavors_order)
        servers_params['count'] = max(sum(test_params['count_per_group']) for test_params in tests_params)

        return instances_params

    def prepare_ansible_test_files(self):
        """Prepares ansible inventory and vars files for the tests
        """
        # set global params for test suite
        if self.testsuite_params.get("_global"):
            ansible_manager.update_vars(vars_path=self._get_vars_path('test'),
                                        params=self.testsuite_params["_global"])

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

        ansible_manager.run_playbook(self.abspath(base_setup_playbook),
                                     inventory_path)

    @teamcity_messages.block("PREPARE TEST ENVIRONMENT")
    def setup_environment(self):
        """ Prepares base test environment
        """
        instances_cfg = instances_manager.get_instances_cfg(self.instances_params,
                                                            self.instances_names)

        if not instances_manager.create(instances_cfg):
            raise RuntimeError("Not all nodes available")

        self.prepare_ansible_test_files()
        self.install_elliptics_packages()

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
    parser.add_argument('--configs-dir', dest="configs_dir", required=True,
                        help="directory with tests' configs")
    parser.add_argument('--testsuite-params', dest="testsuite_params", default=None,
                        help="path to file with parameters which will override default parameters for specified test suite.")
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

    cloud = Cloud(args)
    cloud.setup_environment()
