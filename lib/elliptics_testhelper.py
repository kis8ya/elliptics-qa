# -*- coding: utf-8 -*-
#
import elliptics
import random
import pytest
import os
import subprocess
import shlex

import utils

# offset position (writing)
OffsetWriteGetter = {'BEGINNING':     lambda l: 0,
                     'MIDDLE':        lambda l: random.randint(1, l - 2),
                     'END':           lambda l: random.randint(1, l - 1),
                     'APPENDING':     lambda l: l,
                     'OVER_BOUNDARY': lambda l: random.randint(l + 1, utils.MAX_LENGTH + 2)}

# size value (reading)
SizeGetter = {'NULL':      lambda l, os=None: 0,
              'DATA_SIZE': lambda l, os=None: l,
              'PART':      lambda l, os=None: random.randint(1, l - 1),
              'OVER_SIZE': lambda l, os=None: random.randint(l + 1, utils.MAX_LENGTH + 2),
              'PART_DEPEND_ON_OFFSET_VALID':   lambda l, os=None: random.randint(1, l - os),
              'PART_DEPEND_ON_OFFSET_INVALID': lambda l, os=None: random.randint(l - os + 1, l)}

# offset position (reading)
OffsetReadGetter = {'NULL':          lambda l: 0,
                    'MIDDLE':        lambda l: random.randint(1, l - 1),
                    'OVER_BOUNDARY': lambda l: random.randint(l + 1, utils.MAX_LENGTH + 2)}

# chunk_size (writing)
ChunkSizeGetter = {'NULL':      lambda l: 0,
                   'MIDDLE':    lambda l: random.randint(1, l - 1),
                   'DATA_SIZE': lambda l: l,
                   'OVER_SIZE': lambda l: random.randint(l + 1, utils.MAX_LENGTH + 2)}

# data length (writing with offset)
OffsetDataGetter = {'BEGINNING':     lambda l, os: random.randint(1, l - os - 1),
                    'MIDDLE':        lambda l, os: random.randint(1, l - os - 1),
                    'END':           lambda l, os: l - os,
                    'APPENDING':     lambda l, os: random.randint(l - os + 1, utils.MAX_LENGTH + 1),
                    'OVER_BOUNDARY': lambda l, os: random.randint(1, utils.MAX_LENGTH)}

@pytest.fixture(scope='module')
def nodes(pytestconfig):
    nodes = EllipticsTestHelper.get_nodes_from_args(pytestconfig.option.nodes)
    return nodes

@pytest.fixture(scope='module')
def client(pytestconfig, nodes):
    """Prepares default elliptics session."""
    client = EllipticsTestHelper(nodes=nodes)
    return client

@pytest.fixture(scope='function')
def key_and_data():
    """ Returns key and data (random sequence of bytes)
    """
    return utils.get_key_and_data()

@pytest.fixture(scope='function')
def timestamp():
    """Returns elliptics timestamp."""
    return elliptics.Time.now()

@pytest.fixture(scope='function')
def user_flags():
    """ Returns randomly generated user_flags
    """
    user_flags = random.randint(0, utils.USER_FLAGS_MAX)
    return user_flags

def set_networking_limitations(download=9216, upload=9216):
    """Sets download/upload bandwidth limitation (9 MBit by default)."""
    cmd = "wondershaper eth0 {down} {up}".format(down=download, up=upload)
    subprocess.call(shlex.split(cmd))

def clear_networking_limitations():
    """Clears networking limitations."""
    cmd = "wondershaper clear eth0"
    subprocess.call(shlex.split(cmd))

class EllipticsTestHelper(elliptics.Session):
    """ This class extend elliptics.Session class with some useful (for tests) features
    """
    error_info = type("Errors", (), {
            'WrongArguments': "Argument list too long",
            'NotExists': "No such file or directory",
            'TimeoutError': "Connection timed out",
            'AddrNotExists': "No such device or address"
            })

    _log_path = "/var/log/elliptics/client.log"
    
    DROP_RULES = ["INPUT --proto tcp --destination-port {port} --jump DROP",
                  "INPUT --proto tcp --source-port {port} --jump DROP",
                  "OUTPUT --proto tcp --destination-port {port} --jump DROP",
                  "OUTPUT --proto tcp --source-port {port} --jump DROP"]

    def __init__(self, nodes, wait_timeout=None, check_timeout=None,
                 groups=None, config=None, logging_level=4):
        if logging_level:
            dir_path = os.path.dirname(self._log_path)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            elog = elliptics.Logger(self._log_path, logging_level)
        else:
            elog = elliptics.Logger("/dev/stderr", logging_level)

        if config is None:
            config = elliptics.Config()

        if wait_timeout is not None:
            config.wait_timeout = wait_timeout
        if check_timeout is not None:
            config.check_timeout = check_timeout

        client_node = elliptics.Node(elog, config)
        for node in nodes:
            client_node.add_remote(node.host, node.port)

        elliptics.Session.__init__(self, client_node)

        if groups is None:
            groups = set()
            for n in nodes:
                groups.add(n.group)
            groups = list(groups)

        self.groups = groups

        self.dropped_nodes = []

    @staticmethod
    def get_nodes_from_args(args):
        """ Returns list of nodes from command line arguments
        (option '--node')
        """
        return [utils.Node(*n.split(':')) for n in args]

    def drop_node(self, node):
        """ Makes a node unavailable for elliptics requests
        """
        for drop_rule in EllipticsTestHelper.DROP_RULES:
            rule = drop_rule.format(port=node.port)
            cmd = "ssh -q root@{host} iptables --append {rule}".format(host=node.host,
                                                                       rule=rule)
            subprocess.call(shlex.split(cmd))
            print(cmd)
            self.dropped_nodes.append(node)

    def resume_node(self, node):
        """ Unlocks a node for elliptics requests
        """
        for drop_rule in EllipticsTestHelper.DROP_RULES:
            rule = drop_rule.format(port=node.port)
            cmd = "ssh -q root@{host} iptables --delete {rule}".format(host=node.host,
                                                                       rule=rule)
            subprocess.call(shlex.split(cmd))
            self.dropped_nodes.remove(node)

    def resume_all_nodes(self):
        """ Unlocks all nodes for elliptics requests
        """
        for node in self.dropped_nodes:
            self.resume_node(node)

    # Synchronous versions for Elliptics commands
    def write_data_sync(self, key, data, offset=0, chunk_size=0):
        return self.write_data(key, data, offset=offset, chunk_size=chunk_size).get()

    def read_data_sync(self, key, offset=0, size=0):
        return self.read_data(key, offset=offset, size=size).get()
    
    def write_prepare_sync(self, key, data, offset, psize):
        return self.write_prepare(key, data, offset, psize).get()

    def write_plain_sync(self, key, data, offset):
        return self.write_plain(key, data, offset).get()

    def write_commit_sync(self, key, data, offset, csize):
        return self.write_commit(key, data, offset, csize).get()

    def read_data_from_groups_sync(self, key, groups, offset=0):
        return self.read_data_from_groups(key, groups=groups, offset=offset).get()
    

    def checking_inaccessibility(self, key, data_len=None):
        """ Checking that data is inaccessible
        """
        try:
            result_data = self.read_data_sync(key).pop().data
        except Exception as e:
            print e.message
        else:
            print len(result_data), '/', data_len, 'bytes already accessible'
            assert utils.get_sha1(result_data) != key
