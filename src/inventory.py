#!/usr/bin/env python3
import os.path
import subprocess
import sys

import yaml
from yaml import dump
from simulator.util import exit_with_error

default_inventory_path = 'inventory.yaml'


def find_host_with_public_ip(inventory, public_ip):
    for host in inventory:
        if host["public_ip"] == public_ip:
            return host
    return


def load_hosts(inventory_path=None, host_pattern="all"):
    if not inventory_path:
        inventory_path = default_inventory_path

    if not os.path.exists(inventory_path):
        exit_with_error(f"Could not find [{inventory_path}]")

    cmd = f"ansible  -i {inventory_path}  --list-hosts  '{host_pattern}'"

    lines = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True).stdout.splitlines()

    # first line contains 'hosts ...'. We don't want it.
    lines.pop(0)
    desired_hosts = []
    for line in lines:
        desired_hosts.append(line.strip())
    cmd = f"ansible-inventory -i {inventory_path} -y --list"
    out = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True).stdout

    inventory_yaml = yaml.safe_load(out)
    result = []
    children = inventory_yaml['all']['children']
    for group_name, group in children.items():
        hosts = group.get('hosts')
        if hosts:
            for hostname, host in hosts.items():
                if not hostname in desired_hosts:
                    continue

                new_host = {}
                result.append(new_host)
                new_host['public_ip'] = hostname
                new_host['private_ip'] = host.get('private_ip')
                new_host['ssh_user'] = host.get('ansible_user')
                new_host['groupname'] = group_name
                private_key = host.get("ansible_ssh_private_key_file")
                if private_key:
                    new_host['ssh_options'] = f"-i {private_key} -o StrictHostKeyChecking=no -o ConnectTimeout=60"
    return result


if __name__ == '__main__':
    inventory = load_hosts(host_pattern=sys.argv[1])
    print(dump(inventory))