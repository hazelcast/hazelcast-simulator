import os.path

from inventory import load_hosts
from simulator.driver import find_driver_config_file, DriverConfigureArgs
from simulator.log import info
from simulator.util import read_file


def _configure_hazelcast_xml(nodes, args: DriverConfigureArgs, is_lite_member: bool):
    src_file = args.test.get("hazelcast_xml")
    if src_file is not None:
        if not os.path.exists(src_file):
            raise Exception(f"Could not find hazelcast.xml with path [{src_file}]")
    else:
        src_file = find_driver_config_file(args.driver, "hazelcast.xml")

    config = read_file(src_file)

    # configure <!--LICENSE-KEY-->
    license_key = args.test.get("license_key")
    if license_key is not None:
        license_key_config = f"<license-key>{license_key}</license-key>"
        config = config.replace("<!--LICENSE-KEY-->", license_key_config)

    # configure <!--LITE_MEMBER_CONFIG-->
    if is_lite_member:
        lite_member_config = "<lite-member enabled=\"true\"/>"
    else:
        lite_member_config = "<lite-member enabled=\"false\"/>"

    config = config.replace("<!--LITE_MEMBER_CONFIG-->", lite_member_config)

    # configure <!--MEMBERS-->
    members_config = ""
    member_port = args.test.get('member_port', '5701')
    for host in nodes:
        members_config = f"{members_config}<member>{host['private_ip']}:{member_port}</member>"
    config = config.replace("<!--MEMBERS-->", members_config)

    if is_lite_member:
        args.coordinator_params['file:litemember-hazelcast.xml'] = config
    else:
        args.coordinator_params['file:hazelcast.xml'] = config


def _configure_client_hazelcast_xml(nodes, args: DriverConfigureArgs):
    src_file = args.test.get("client_hazelcast_xml")
    if src_file is not None:
        if not os.path.exists(src_file):
            raise Exception(f"Could not find client-hazelcast.xml with path [{src_file}]")
    else:
        src_file = find_driver_config_file(args.driver, "client-hazelcast.xml")

    config = read_file(src_file)

    members_config = ""
    member_port = args.test.get('member_port', '5701')
    for host in nodes:
        members_config = f"{members_config}<address>{host['private_ip']}:{member_port}</address>"

    config = config.replace("<!--MEMBERS-->", members_config)
    args.coordinator_params['file:client-hazelcast.xml'] = config


def _configure_log4j_xml(args: DriverConfigureArgs):
    driver = args.driver
    log4j_xml = read_file(find_driver_config_file(driver, "log4j.xml"))
    args.coordinator_params['file:log4j.xml'] = log4j_xml


def _configure_worker_sh(args: DriverConfigureArgs):
    driver = args.driver

    worker_sh_path = args.test.get('worker_sh')
    if worker_sh_path is None:
        worker_sh = read_file(find_driver_config_file(driver, "worker.sh"))
    else:
        worker_sh = read_file(worker_sh_path)

    args.coordinator_params['file:worker.sh'] = worker_sh

def exec(args: DriverConfigureArgs):
    info("Configure")

    nodes_pattern = args.test.get("node_hosts")
    nodes = load_hosts(inventory_path=args.inventory_path, host_pattern=nodes_pattern)

    node_count = args.test.get("node_count")
    if node_count is not None:
        # For Hazelcast we need to have workers for both clients (load generators) and
        # nodes (members). By setting the NODE_WORKER_COUNT, we control the number of
        # passive member workers that are started.
        # There are also drivers like Redis OS that doesn't require any workers for the
        # server, so NODE_WORKER_COUNT should be zero (which is the default).
        args.coordinator_params['NODE_WORKER_COUNT'] = node_count

    _configure_log4j_xml(args)
    _configure_worker_sh(args)
    _configure_hazelcast_xml(nodes, args, False)
    _configure_hazelcast_xml(nodes, args, True)
    _configure_client_hazelcast_xml(nodes, args)

    info("Configure: done")
