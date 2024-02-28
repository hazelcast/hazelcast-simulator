#!/usr/bin/env python3

import os.path

from inventory import load_hosts
from simulator.util import find_config_file, find_driver_config_file, \
    read_file


def configure_hazelcast_xml(nodes, test_yaml, is_server, params):
    src_file = test_yaml.get("hazelcast_xml")
    if src_file is not None:
        if not os.path.exists(src_file):
            raise Exception(f"Could not find hazelcast.xml with path [{src_file}]")
    else:
        src_file = find_driver_config_file(driver, "hazelcast.xml")

    config = read_file(src_file)

    # configure <!--LICENSE-KEY-->
    license_key = test_yaml.get("license_key")
    if license_key is not None:
        license_key_config = f"<license-key>{license_key}</license_key>"
        config = config.replace("<!--LICENSE-KEY-->", license_key_config)

    # configure <!--LITE_MEMBER_CONFIG-->
    if is_server:
        lite_member_config = "<lite-member enabled=\"false\"/>"
    else:
        lite_member_config = "<lite-member enabled=\"true\"/>"

    config = config.replace("<!--LITE_MEMBER_CONFIG-->", lite_member_config)

    # configure <!--MEMBERS-->
    members_config = ""
    member_port = test_yaml.get('member_port')
    if member_port is None:
        member_port = "5701"
    for host in nodes:
        members_config = f"{members_config}<member>{host['private_ip']}:{member_port}</member>"
    config = config.replace("<!--MEMBERS-->", members_config)

    if is_server:
        params['file:hazelcast.xml'] = config
    else:
        params['file:litemember-hazelcast.xml'] = config


def configure_client_hazelcast_xml(nodes, test_yaml, params):
    src_file = test_yaml.get("client_hazelcast_xml")
    if src_file is not None:
        if not os.path.exists(src_file):
            raise Exception(f"Could not find client-hazelcast.xml with path [{src_file}]")
    else:
        src_file = find_driver_config_file(driver, "client-hazelcast.xml")

    config = read_file(src_file)

    members_config = ""
    member_port = test_yaml.get('member_port')
    if member_port is None:
        member_port = "5701"
    for host in nodes:
        members_config = f"{members_config}<address>{host['private_ip']}:{member_port}</address>"

    config = config.replace("<!--MEMBERS-->", members_config)
    params['file:client_hazelcast.xml']=config



def configure_log4j_xml(is_server, params):
    log4j_xml = read_file(find_config_file("worker-log4j.xml"))
    if is_server:
        params['server:file:log4j.xml'] = log4j_xml
    else:
        params['client:file:log4j.xml'] = log4j_xml


def configure_worker_sh(is_server, params):
    src_worker_sh = read_file(find_config_file("worker.sh"))
    if is_server:
        params['server:worker.sh'] = src_worker_sh
    else:
        params['client:worker.sh'] = src_worker_sh


def exec(test_yaml, is_server, inventory_path, params):
    global driver
    driver = test_yaml.get('driver')

    nodes_pattern = test_yaml.get("node_hosts")
    if nodes_pattern is None:
        nodes_pattern = "nodes"

    nodes = load_hosts(inventory_path=inventory_path, host_pattern=nodes_pattern)

    configure_log4j_xml(is_server, params)
    configure_worker_sh(is_server, params)

    if is_server:
        configure_hazelcast_xml(nodes, test_yaml, True, params)
    else:
        client_type = test_yaml.get('client_type')
        if client_type is None:
            client_type = 'javaclient'

        if client_type == 'javaclient':
            configure_client_hazelcast_xml(nodes, test_yaml, params)
        elif client_type == 'litemember':
            configure_hazelcast_xml(nodes, test_yaml, False, params)
        else:
            raise Exception(f"Unrecognized client_type {client_type}")
