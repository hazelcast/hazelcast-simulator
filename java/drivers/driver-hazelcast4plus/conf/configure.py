import os.path

from inventory import load_hosts
from simulator.driver import find_driver_config_file, DriverConfigureArgs
from simulator.util import read_file


def _configure_hazelcast_xml(nodes, args: DriverConfigureArgs):
    src_file = args.test.get("hazelcast_xml")
    if src_file is not None:
        if not os.path.exists(src_file):
            raise Exception(f"Could not find hazelcast.xml with path [{src_file}]")
    else:
        src_file = find_driver_config_file(_get_driver(args), "hazelcast.xml")

    config = read_file(src_file)

    # configure <!--LICENSE-KEY-->
    license_key = args.test.get("license_key")
    if license_key is not None:
        license_key_config = f"<license-key>{license_key}</license-key>"
        config = config.replace("<!--LICENSE-KEY-->", license_key_config)

    # configure <!--LITE_MEMBER_CONFIG-->
    if args.is_passive:
        lite_member_config = "<lite-member enabled=\"false\"/>"
    else:
        lite_member_config = "<lite-member enabled=\"true\"/>"

    config = config.replace("<!--LITE_MEMBER_CONFIG-->", lite_member_config)

    # configure <!--MEMBERS-->
    members_config = ""
    member_port = args.test.get('member_port')
    if member_port is None:
        member_port = "5701"
    for host in nodes:
        members_config = f"{members_config}<member>{host['private_ip']}:{member_port}</member>"
    config = config.replace("<!--MEMBERS-->", members_config)

    if args.is_passive:
        args.coordinator_params['file:hazelcast.xml'] = config
    else:
        args.coordinator_params['file:litemember-hazelcast.xml'] = config


def _configure_client_hazelcast_xml(nodes, args: DriverConfigureArgs):
    src_file = args.test.get("client_hazelcast_xml")
    if src_file is not None:
        if not os.path.exists(src_file):
            raise Exception(f"Could not find client-hazelcast.xml with path [{src_file}]")
    else:
        src_file = find_driver_config_file(_get_driver(args), "client-hazelcast.xml")

    config = read_file(src_file)

    members_config = ""
    member_port = args.test.get('member_port')
    if member_port is None:
        member_port = "5701"
    for host in nodes:
        members_config = f"{members_config}<address>{host['private_ip']}:{member_port}</address>"

    config = config.replace("<!--MEMBERS-->", members_config)
    args.coordinator_params['file:client-hazelcast.xml'] = config


def _configure_log4j_xml(args: DriverConfigureArgs):
    driver = _get_driver(args)
    log4j_xml = read_file(find_driver_config_file(driver, "worker-log4j.xml"))
    if args.is_passive:
        args.coordinator_params['file:server-log4j.xml'] = log4j_xml
    else:
        args.coordinator_params['file:client-log4j.xml'] = log4j_xml


def _configure_worker_sh(args: DriverConfigureArgs):
    driver = _get_driver(args)

    if args.is_passive:
        worker_sh = read_file(find_driver_config_file(driver, "worker.sh"))
        args.coordinator_params['file:server-worker.sh'] = worker_sh
    else:
        worker_sh = read_file(find_driver_config_file(driver, "worker.sh"))
        args.coordinator_params['file:client-worker.sh'] = worker_sh


def _get_driver(args):
    return args.test.get('driver')


def exec(args: DriverConfigureArgs):
    print("[INFO] Configure")

    nodes_pattern = args.test.get("node_hosts", 'nodes')
    nodes = load_hosts(inventory_path=args.inventory_path, host_pattern=nodes_pattern)

    _configure_log4j_xml(args)
    _configure_worker_sh(args)

    if args.is_passive:
        _configure_hazelcast_xml(nodes, args)
    else:
        client_type = args.test.get('client_type', 'javaclient')

        if client_type == 'javaclient':
            _configure_client_hazelcast_xml(nodes, args)
        elif client_type == 'litemember':
            _configure_hazelcast_xml(nodes, args)
        else:
            raise Exception(f"Unrecognized client_type {client_type}")

    print("[INFO] Configure: done")
