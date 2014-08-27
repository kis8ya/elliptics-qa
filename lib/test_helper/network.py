"""This module helps to work with a test bench on a network level."""

import subprocess
import shlex

from test_helper.logging_tests import logger


def add_scheduler(host):
    """Adds a scheduler for a network interface on specified host."""
    cmd = "ssh -q {} tc qdisc add dev eth0 root netem delay 0ms".format(host)
    subprocess.check_call(shlex.split(cmd))
    logger.info("A scheduler for network emulator was added for host: {}\n".format(host))


def set_networking_delay(host, delay):
    """Sets delay on a network interface for specified host (in milliseconds)."""
    cmd = "ssh -q {} tc qdisc change dev eth0 root netem delay {}ms".format(host, delay)
    subprocess.check_call(shlex.split(cmd))
    logger.info("A networking delay for host {} was set to {} ms\n".format(host, delay))


def del_scheduler(host):
    """Removes a scheduler for a network interface on specified host."""
    cmd = "ssh -q {} tc qdisc del dev eth0 root netem delay 0ms".format(host)
    subprocess.check_call(shlex.split(cmd))
    logger.info("A sceduler for network emulator was removed for host: {}\n".format(host))
