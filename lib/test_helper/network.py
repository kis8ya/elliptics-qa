"""This module helps to work with a test bench on a network level."""
import elliptics
import socket
import time
import subprocess
import shlex

from test_helper import ssh
from test_helper.logging_tests import logger


DROP_RULES = ["INPUT --proto tcp --destination-port {port} --jump DROP",
              "INPUT --proto tcp --source-port {port} --jump DROP",
              "OUTPUT --proto tcp --destination-port {port} --jump DROP",
              "OUTPUT --proto tcp --source-port {port} --jump DROP"]


def add_scheduler(host):
    """Adds a scheduler for a network interface on specified host."""
    sshclient = ssh.get_sshclient(host)
    cmd = "sudo tc qdisc add dev eth0 root netem delay 0ms"
    ssh.exec_command(sshclient, cmd)
    logger.info("A scheduler for network emulator was added for host: {}\n".format(host))


def set_networking_delay(host, delay):
    """Sets delay on a network interface for specified host (in milliseconds)."""
    sshclient = ssh.get_sshclient(host)
    cmd = "sudo tc qdisc change dev eth0 root netem delay {}ms".format(delay)
    ssh.exec_command(sshclient, cmd)
    logger.info("A networking delay for host {} was set to {} ms\n".format(host, delay))


def del_scheduler(host):
    """Removes a scheduler for a network interface on specified host."""
    sshclient = ssh.get_sshclient(host)
    cmd = "sudo tc qdisc del dev eth0 root netem delay 0ms"
    ssh.exec_command(sshclient, cmd)
    logger.info("A scheduler for network emulator was removed for host: {}\n".format(host))


def drop_node(node):
    """Drops specified node through firewall."""
    sshclient = ssh.get_sshclient(node.host)
    for drop_rule in DROP_RULES:
        rule = drop_rule.format(port=node.port)
        cmd = "sudo iptables --append {rule}".format(rule=rule)

        ssh.exec_command(sshclient, cmd)
        logger.info("{}: {}\n".format(node.host, cmd))


def resume_node(node):
    """Resumes specified node through firewall."""
    sshclient = ssh.get_sshclient(node.host)
    for drop_rule in DROP_RULES:
        rule = drop_rule.format(port=node.port)
        cmd = "sudo iptables --delete {rule}".format(rule=rule)

        ssh.exec_command(sshclient, cmd)
        logger.info("{}: {}\n".format(node.host, cmd))


def _wait_stall_counter(session, nodes):
    """Waits untill stall counter reaches `stall_count`."""
    STALL_COUNT = 3
    addresses = [elliptics.Address(node.host, node.port, socket.AF_INET)
                 for node in nodes]

    async_results = []
    for _ in xrange(STALL_COUNT):
        for address in addresses:
            async_results.append(session.request_backends_status(address))
        # Wait a bit, to count these transaction separatly for stall counter
        time.sleep(1)

    # Wait for completion of all transactions
    for async_result in async_results:
        try:
            async_result.wait()
        except elliptics.Error:
            pass

    # Wait check-request from elliptics client node itself
    # (by doing another request and waiting it)
    try:
        session.request_backends_status(address).wait()
    except elliptics.Error:
        pass


def drop_nodes_and_wait(nodes, session):
    """Disables nodes through a firewall and wait for connections termination."""
    for node in nodes:
        drop_node(node)

    # Wait when connections to nodes will be terminated
    _wait_stall_counter(session, nodes)


def resume_nodes_and_wait(nodes, check_timeout):
    for node in nodes:
        resume_node(node)

    wait_time = check_timeout + 1
    logger.info("Wait {} seconds for routing table update...".format(wait_time))
    time.sleep(wait_time)


def set_networking_limitations(download=9216, upload=9216):
    """Sets download/upload bandwidth limitation (9 MBit by default)."""
    cmd = "sudo wondershaper eth0 {down} {up}".format(down=download, up=upload)
    subprocess.check_call(shlex.split(cmd))


def clear_networking_limitations():
    """Clears networking limitations."""
    cmd = "sudo wondershaper clear eth0"
    subprocess.check_call(shlex.split(cmd))
