#!/usr/bin/env python3

import os.path
import sys

from inventory import load_hosts
from simulator.util import load_yaml_file, copy_file, find_config_file, find_driver_config_file, \
    read_file, write_file, parse_bool


def generate_hazelcast_xml(lite_member=False):
    src_file = test_yaml.get("hazelcast_xml")
    if src_file is not None:
        if not os.path.exists(src_file):
            raise Exception(f"Could not find hazelcast.xml with path [{src_file}]")
    else:
        src_file = find_driver_config_file(driver, "hazelcast.xml")

    dst_file = f"{specific_coordinator_props_dir}/hazelcast.xml"
    template = read_file(src_file)

    # configure <!--LICENSE-KEY-->
    license_key = test_yaml.get("license_key")
    if license_key is not None:
        license_key_config = f"<license-key>{license_key}</license_key>"
        template = template.replace("<!--LICENSE-KEY-->", license_key_config)

    # configure <!--LITE_MEMBER_CONFIG-->
    if lite_member:
        lite_member_config = "<lite-member enabled=\"true\"/>"
    else:
        lite_member_config = "<lite-member enabled=\"false\"/>"
    template = template.replace("<!--LITE_MEMBER_CONFIG-->", lite_member_config)

    # configure <!--MEMBERS-->
    members_config = ""
    member_port = test_yaml.get('member_port')
    if member_port is None:
        member_port = "5701"
    for host in nodes:
        members_config = f"{members_config}<member>{host['private_ip']}:{member_port}</member>"
    template = template.replace("<!--MEMBERS-->", members_config)

    write_file(dst_file, template)



def generate_client_hazelcast_xml():
    src_file = test_yaml.get("client_hazelcast_xml")
    if src_file is not None:
        if not os.path.exists(src_file):
            raise Exception(f"Could not find client-hazelcast.xml with path [{src_file}]")
    else:
        src_file = find_driver_config_file(driver, "client-hazelcast.xml")

    dst_file = f"{specific_coordinator_props_dir}/client-hazelcast.xml"
    template = read_file(src_file)

    members_config = ""
    member_port = test_yaml.get('member_port')
    if member_port is None:
        member_port = "5701"
    for host in nodes:
        members_config = f"{members_config}<address>{host['private_ip']}:{member_port}</address>"

    template = template.replace("<!--MEMBERS-->", members_config)

    write_file(dst_file, template)


def generate_log4j_xml():
    src_log4j_xml = find_config_file("worker-log4j.xml")
    copy_file(src_log4j_xml, f"{specific_coordinator_props_dir}/worker.log4j")


def generate_worker_sh():
    src_worker_sh = find_config_file("worker.sh")
    copy_file(src_worker_sh, f"{specific_coordinator_props_dir}/worker.sh")


def exec(test_yaml__, is_server__, inventory_path, coordinator_props_dir):
    global test_yaml
    test_yaml = test_yaml__

    global driver
    driver = test_yaml.get('driver')

    global is_server
    is_server = is_server__

    nodes_pattern = test_yaml.get("node_hosts")
    if nodes_pattern is None:
        nodes_pattern = "nodes"

    global nodes
    nodes = load_hosts(inventory_path=inventory_path, host_pattern=nodes_pattern)

    mode = "server" if is_server else "client"
    global specific_coordinator_props_dir
    specific_coordinator_props_dir = f"{coordinator_props_dir}/{mode}"
    os.makedirs(specific_coordinator_props_dir)

    generate_log4j_xml()
    generate_worker_sh()
    if is_server:
        generate_hazelcast_xml()
    else:
        client_type = test_yaml.get('client_type')
        if client_type is None:
            client_type = 'javaclient'

        if client_type == 'javaclient':
            generate_client_hazelcast_xml()
        elif client_type == 'litemember':
            generate_hazelcast_xml(lite_member=True)
        else:
            raise Exception(f"Unrecognized client_type {client_type}")
