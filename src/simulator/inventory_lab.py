import yaml
import os

__SUPPORTED_BANDWIDTHS = frozenset([10, 40])
__INVENTORY_FILE = "inventory.yaml"


def lab_destroy():
    try:
        os.remove(__INVENTORY_FILE)
    except OSError:
        pass


def lab_apply(inventory_plan):
    specified_machines = inventory_plan.get("machines")
    machine_range = specified_machines if specified_machines else range(1, 11)
    lab_addresses = [f"10.212.1.1{n:02}" for n in machine_range]
    bandwidth = int(inventory_plan["bandwidthGbps"])
    if bandwidth not in __SUPPORTED_BANDWIDTHS:
        raise Exception(
            f"Only supported bandwidths are {__SUPPORTED_BANDWIDTHS}")
    output = {}
    for group in ["nodes", "loadgenerators", "mc"]:
        group_plan = inventory_plan[group]
        group_inventory = __configure_group_inventory(
            group_plan, lab_addresses, bandwidth)
        if len(group_inventory) > 0:
            output[group] = {"hosts": group_inventory}
    with open(__INVENTORY_FILE, 'w') as f:
        f.write(yaml.dump(output))


def __configure_group_inventory(group_plan, lab_addresses, bandwidth):
    output = {}
    group_size = group_plan["count"]
    for i in range(0, group_size):
        if len(lab_addresses) == 0:
            raise Exception("Not enough lab machines for your configuration")
        host_address = lab_addresses.pop(0)
        output[host_address] = {
            "ansible_ssh_private_key_file": "key",
            "ansible_user": group_plan["user"],
            "private_ip": f"{host_address[:7]}{bandwidth}{host_address[8:]}"
        }
    return output
