# -*- coding: utf-8 -*-
from __future__ import print_function

import os
import subprocess
import shlex
import signal
import socket

from collections import deque
from functools import wraps

os_login = os.environ.get('OS_USERNAME', None)
os_password = os.environ.get('OS_PASSWORD', None)
os_url = os.environ.get('OS_URL', None)
os_tenant_name = os.environ.get('OS_TENANT_NAME', None)

TIMEOUT = 60

ENDPOINTS_INFO = {"COMPUTE": {'port': 443,
                              'host': "https://compute-sas1.fog.yandex-team.ru",
                              'uri': {"IMAGES": 'v2/{tenant_id}/images',
                                      "FLAVORS": 'v2/{tenant_id}/flavors/detail',
                                      "NETWORKS": 'v2/{tenant_id}/os-networks',
                                      "SERVERS": 'v2/{tenant_id}/servers',
                                      "SERVERS_SERVER": 'v2/{tenant_id}/servers/{instance_id}',
                                      "ACTION": 'v2/{tenant_id}/servers/{server_id}/action'}},
                  "IDENTITY": {'port': 443,
                               'host': "https://auth.fog.yandex-team.ru",
                               'uri': {"TOKENS": 'v2.0/tokens'}}}

class ApiRequestError(Exception):
    pass

class TimeoutError(Exception):
    pass

def _alarm_handler(signal, frame):
    raise TimeoutError()

def with_timeout(func):
    def _decorator(self, *args, **kwargs):
        # set timeout
        signal.signal(signal.SIGALRM, _alarm_handler)
        # to 5 minutes
        signal.alarm(300)

        result = func(self, *args, **kwargs)

        # turn off timer when the function processed
        signal.alarm(0)

        return result
    return wraps(func)(_decorator)

@with_timeout
def wait_till_active(session, instances):
    """ Waits till instances will be in ACTIVE status
    (and returns a list of dicts {instance_name: ip})
    """
    hosts_ip = {}
    queue = deque(instances)
    while queue:
        instance = queue.pop()
        instance_info = session.get_instance_info(instance)

        if instance_info['status'] == "ACTIVE":
            # get ip address
            network_name = instance_info['addresses'].keys()[0]
            hosts_ip[instance+'.i.fog.yandex.net'] = instance_info['addresses'][network_name][0]['addr']
            continue
        queue.appendleft(instance)
    return hosts_ip

@with_timeout
def check_ssh_port(ip_list):
    """ Checks that instances' ssh ports are available
    """
    queue = deque(ip_list)
    while queue:
        ip = queue.pop()
        # availability check
        cmd = "nc -z -w1 {0} 22".format(ip)
        if subprocess.call(shlex.split(cmd)) == 0:
            continue
        # if it's not available yet then return the instance to the queue
        queue.appendleft(ip)

@with_timeout
def check_host_name_resolving(hosts_ip):
    """ Checks that ip resolving returns the same ip
    (which one we got from OpenStack API)
    """
    queue = deque(hosts_ip.items())
    while queue:
        host, ip = queue.pop()
        try:
            resolved_ip = socket.gethostbyname(host)
            if resolved_ip == ip:
                continue
        except socket.error:
            pass
        queue.appendleft((host, ip))

def check_availability(session, instances):
    """ Checks that instances are available
    """
    try:
        print("Waiting for nodes to initialize...", end=' ')
        hosts_ip = wait_till_active(session, instances)
        print("[DONE]")

        print("Waiting for nodes to become available via SSH...", end=' ')
        check_ssh_port(hosts_ip.values())
        print("[DONE]")

        print("Waiting for nodes to start resolving to right IPs...", end=' ')
        check_host_name_resolving(hosts_ip)
        print("[DONE]")

        return True
    except TimeoutError:
        print("[FAILED] Timeout reached.")
        return False

def get_instances_names_from_conf(instance_cfg):
    """ Returns list of instances' names
    """
    name = instance_cfg['name']
    count = instance_cfg['max_count']
    # generate the names
    if count == 1:
        instances = [name]
    else:
        # add -N suffix if max_count != 1
        # (where N is an instance number)
        instances = [ name + '-' + str(i) for i in range(1, count + 1) ]
    return instances

def get_url(service_type, endpoint_type="COMPUTE", **kwargs):
    """ Returns Service Endpoint URL
    """
    url = '{host}:{port}/{uri}'.format(
        host=ENDPOINTS_INFO[endpoint_type]['host'],
        port=ENDPOINTS_INFO[endpoint_type]['port'],
        uri=ENDPOINTS_INFO[endpoint_type]['uri'][service_type])
    url = url.format(**kwargs)
    return url
