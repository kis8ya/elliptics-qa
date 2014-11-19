import paramiko
import os
import os.path
import time


def _get_identityfile_from_config(host, config_path="~/.ssh/config"):
    config_full_path = os.path.abspath(os.path.expanduser(config_path))
    sshconf = paramiko.SSHConfig()
    sshconf.parse(open(config_full_path))
    host_ssh_settings = sshconf.lookup(host)
    private_key = host_ssh_settings["identityfile"]
    return private_key


def _recv_all_stderr(ssh_channel):
    buffer_len = 1024
    stderr = ""
    buff = ssh_channel.recv_stderr(buffer_len)
    while buff:
        stderr += buff
        buff = ssh_channel.recv_stderr(buffer_len)
    return stderr


def _wait_for_command(cmd, stderr):
    """Wait for command to complete."""
    while not stderr.channel.exit_status_ready():
        time.sleep(0.1)

    if stderr.channel.exit_status:
        stderr_output = _recv_all_stderr(stderr.channel)
        raise RuntimeError("Got an error:\n"
                           "Command: {}\n"
                           "Exit status: {}\n"
                           "stderr:\n{}".format(cmd, stderr.channel.exit_status, stderr_output))


def get_sshclient(host):
    """Returns ssh client connected to specified host."""
    # Set logging for ssh client
    ssh_logging_file_path = "/var/log/elliptics_testing/ssh.log"
    dir_path = os.path.dirname(ssh_logging_file_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    paramiko.util.log_to_file(ssh_logging_file_path)

    sshclient = paramiko.SSHClient()
    sshclient.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    private_key = _get_identityfile_from_config(host)

    sshclient.connect(host, key_filename=private_key)

    return sshclient


def exec_command(sshclient, cmd):
    """Wrapper for `paramiko.SSHClient.exec_command`, which is doing blocking call."""
    _, stdout, stderr = sshclient.exec_command(cmd)
    _wait_for_command(cmd, stderr)
    return stdout, stderr
