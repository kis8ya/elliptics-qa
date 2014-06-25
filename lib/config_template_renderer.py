"""Test configuration templates rendering module.

This module processes rendering for test configuration files with Jinja2 templates.
It allows to use clients and servers variables in these test configs as well as
any variable from **params** test config's section.

"""

import json
import copy

from jinja2 import Environment, FileSystemLoader

import ansible_manager
from utils import Node

class Client(object):
    """Clients object for testing needs."""
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def __repr__(self):
        return "Client({host}, {port})".format(**self.__dict__)

_hostname_template = "{0}.i.fog.yandex.net"

def _get_clients(clients_count, instances_names):
    client_port = 1083
    clients_names = ansible_manager.get_host_names(instances_names['client'],
                                                   clients_count)
    clients = []
    for client in clients_names:
        host = _hostname_template.format(client)
        clients.append(Client(host, client_port))

    return clients

def _get_servers(servers_per_group, instances_names):
    servers = []
    server_port = 1025
    servers_names = ansible_manager.get_host_names(instances_names['server'],
                                                   sum(servers_per_group))
    server_name = iter(servers_names)
    for group in xrange(len(servers_per_group)):
        for i in xrange(servers_per_group[group]):
            host = _hostname_template.format(next(server_name))
            servers.append(Node(host, server_port, group+1))

    return servers

def get_cfg(path, instances_names):
    """Returns test config as dictionary."""
    # Getting information about clients and servers
    tmp_cfg = json.load(open(path))
    variables = copy.deepcopy(tmp_cfg["params"])
    variables["clients"] = _get_clients(tmp_cfg["test_env_cfg"]["clients"]["count"],
                                        instances_names)
    variables["servers"] = _get_servers(tmp_cfg["test_env_cfg"]["servers"]["count_per_group"],
                                        instances_names)
    # Render test config
    environment = Environment(loader=FileSystemLoader('/'))
    template = environment.get_template(path)
    out = template.render(**variables)

    cfg = json.loads(out)

    return cfg
