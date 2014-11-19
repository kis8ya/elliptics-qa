"""This module helps to work with a test bench on a network level."""

from test_helper import ssh
from test_helper.logging_tests import logger


DROP_RULES = ["INPUT --proto tcp --destination-port {port} --jump DROP",
              "INPUT --proto tcp --source-port {port} --jump DROP",
              "OUTPUT --proto tcp --destination-port {port} --jump DROP",
              "OUTPUT --proto tcp --source-port {port} --jump DROP"]


def add_scheduler(host):
    """Adds a scheduler for a network interface on specified host."""
    sshclient = ssh.get_sshclient(host)
    cmd = "ssh -q {} tc qdisc add dev eth0 root netem delay 0ms".format(host)
    ssh.exec_command(sshclient, cmd)
    logger.info("A scheduler for network emulator was added for host: {}\n".format(host))


def set_networking_delay(host, delay):
    """Sets delay on a network interface for specified host (in milliseconds)."""
    sshclient = ssh.get_sshclient(host)
    cmd = "ssh -q {} tc qdisc change dev eth0 root netem delay {}ms".format(host, delay)
    ssh.exec_command(sshclient, cmd)
    logger.info("A networking delay for host {} was set to {} ms\n".format(host, delay))


def del_scheduler(host):
    """Removes a scheduler for a network interface on specified host."""
    sshclient = ssh.get_sshclient(host)
    cmd = "ssh -q {} tc qdisc del dev eth0 root netem delay 0ms".format(host)
    ssh.exec_command(sshclient, cmd)
    logger.info("A scheduler for network emulator was removed for host: {}\n".format(host))


def drop_node(node):
    """Drops specified node through firewall."""
    sshclient = ssh.get_sshclient(node.host)
    for drop_rule in DROP_RULES:
        rule = drop_rule.format(port=node.port)
        cmd = "iptables --append {rule}".format(host=node.host, rule=rule)

        ssh.exec_command(sshclient, cmd)
        logger.info("{}: {}\n".format(node.host, cmd))


def resume_node(node):
    """Resumes specified node through firewall."""
    sshclient = ssh.get_sshclient(node.host)
    for drop_rule in DROP_RULES:
        rule = drop_rule.format(port=node.port)
        cmd = "iptables --delete {rule}".format(host=node.host, rule=rule)

        ssh.exec_command(sshclient, cmd)
        logger.info("{}: {}\n".format(node.host, cmd))
