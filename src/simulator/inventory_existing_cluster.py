import yaml
import os
import glob
import re

__INVENTORY_FILE = "inventory.yaml"
__DEFAULT_NODE_USER = "root"

def existing_cluster_destroy():
    """
    Removes the generated inventory.yaml file if it exists.
    Used to clean up the environment configuration for an existing cluster.
    """
    try:
        os.remove(__INVENTORY_FILE)
    except OSError:
        pass

def existing_cluster_apply(inventory_plan):
    """
    Applies the given inventory plan for an existing cluster:
    - Generates inventory.yaml (only from loadgenerators section)
    - Substitutes the cluster name in client-hazelcast.xml
    - Inserts cluster member addresses into client-hazelcast.xml from the nodes list
    """
    cluster_name = inventory_plan.get("cluster_name", "default-cluster")

    # Generate inventory.yaml
    inventory = __generate_inventory_for_existing_cluster(inventory_plan)
    __write_yaml(__INVENTORY_FILE, inventory)

    # Replace placeholders in files
    __substitute_cluster_name(cluster_name)

    # Insert cluster members into client-hazelcast.xml
    __insert_cluster_members(inventory_plan.get("nodes", []))

def __generate_inventory_for_existing_cluster(plan):
    """
    Generates the inventory dictionary for loadgenerators only (no nodes section).
    The result is written to inventory.yaml.
    """
    def format_hosts(entries):
        # For loadgenerators: list of dicts
        if entries and isinstance(entries[0], dict):
            return {
                entry["public_ip"]: {
                    "ansible_ssh_private_key_file": "key",
                    "ansible_user": entry["user"],
                    "private_ip": entry["private_ip"]
                }
                for entry in entries
            }
        else:
            return {}

    return {
        "loadgenerators": {"hosts": format_hosts(plan.get("loadgenerators", []))}
    }

def __write_yaml(path, data):
    with open(path, 'w') as f:
        yaml.dump(data, f, default_flow_style=False)

def __substitute_cluster_name(cluster_name):
    """
    Replaces the <cluster-name>default-cluster</cluster-name> placeholder in client-hazelcast.xml
    with the actual cluster name from the inventory plan.
    """
    placeholder_start = "<cluster-name>"
    placeholder_end = "</cluster-name>"
    file_name = "client-hazelcast.xml"

    if not os.path.isfile(file_name):
        raise FileNotFoundError(f"{file_name} not found in current directory: {os.getcwd()}")

    with open(file_name, "r") as file:
        content = file.read()

    if placeholder_start in content:
        new_content = content.replace(
            f"{placeholder_start}default-cluster{placeholder_end}",
            f"{placeholder_start}{cluster_name}{placeholder_end}"
        )
        with open(file_name, "w") as file:
            file.write(new_content)

def __insert_cluster_members(nodes):
    """
    Inserts <address> entries for each node into client-hazelcast.xml at the <!--MEMBERS--> placeholder.
    Supports both 'ip' and 'ip:port' formats. Defaults to port 5701 if not specified.
    """
    file_name = "client-hazelcast.xml"
    placeholder = "<!--MEMBERS-->"
    if not os.path.isfile(file_name):
        raise FileNotFoundError(f"{file_name} not found in current directory: {os.getcwd()}")

    with open(file_name, "r") as file:
        lines = file.readlines()

    # Find the indentation of the placeholder
    indent = ""
    for line in lines:
        if placeholder in line:
            indent = line[:line.index(placeholder)]
            break

    addresses = []
    for node in nodes:
        if ":" in node:
            ip, port = node.split(":", 1)
            ip = ip.strip()
            port = port.strip()
        else:
            ip = node.strip()
            port = "5701"
        addresses.append(f'{indent}<address>{ip}:{port}</address>\n')

    # Build new content, replacing the placeholder line with the addresses
    new_lines = []
    for line in lines:
        if placeholder in line:
            new_lines.extend(addresses)
        else:
            new_lines.append(line)

    with open(file_name, "w") as file:
        file.writelines(new_lines)
