import yaml
import os
import glob
import re

__INVENTORY_FILE = "inventory.yaml"
# Get absolute path of current script's directory
__SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Build path to agent_start relative to this script
__AGENT_START_PATH = os.path.join(__SCRIPT_DIR, "..", "..", "bin", "hidden", "agent_start")
__AGENT_START_PATH = os.path.abspath(__AGENT_START_PATH)  # Clean up the path


def existing_cluster_destroy():
    try:
        os.remove(__INVENTORY_FILE)
    except OSError:
        pass

def existing_cluster_apply(inventory_plan):

    cluster_name = inventory_plan.get("cluster_name", "default-cluster")

    # Generate inventory.yaml
    inventory = __generate_inventory_for_existing_cluster(inventory_plan)
    __write_yaml(__INVENTORY_FILE, inventory)

    # Replace placeholders in files
    __substitute_cluster_name(cluster_name)

    # Replace java kill block in script
    __replace_java_kill_block(__AGENT_START_PATH)

def __generate_inventory_for_existing_cluster(plan):
    def format_hosts(entries):
        return {
            entry["public_ip"]: {
                "ansible_ssh_private_key_file": "key",
                "ansible_user": entry["user"],
                "private_ip": entry["private_ip"]
            }
            for entry in entries
        }

    return {
        "loadgenerators": {"hosts": format_hosts(plan.get("loadgenerators", []))},
        "nodes": {"hosts": format_hosts(plan.get("nodes", []))}
    }

def __write_yaml(path, data):
    with open(path, 'w') as f:
        yaml.dump(data, f, default_flow_style=False)

def __substitute_cluster_name(cluster_name):
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


def __replace_java_kill_block(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    inside_block = False
    new_lines = []
    for line in lines:
        if re.match(r'\s*if\s+hash\s+killall', line):
            inside_block = True
            new_lines.append("pkill -9 -f 'java.*hazelcast-simulator.*' || true\n")
            continue
        if inside_block and re.match(r'\s*fi\s*', line):
            inside_block = False
            continue
        if not inside_block:
            new_lines.append(line)

    with open(file_path, 'w') as file:
        file.writelines(new_lines)
