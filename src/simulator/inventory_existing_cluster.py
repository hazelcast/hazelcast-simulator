import yaml
import os
import glob
import re

__INVENTORY_FILE = "inventory.yaml"
__AGENT_START_PATH = os.path.join("..", "bin", "hidden", "agent_start")


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

def __substitute_cluster_name(cluster_name, search_dir="."):
    placeholder_start = "<cluster-name>"
    placeholder_end = "</cluster-name>"
    for filepath in glob.glob(os.path.join(search_dir, "**"), recursive=True):
        if not os.path.isfile(filepath) or filepath.endswith(".yaml"):
            continue
        with open(filepath, "r") as file:
            content = file.read()
        if placeholder_start in content:
            new_content = content.replace(
                f"{placeholder_start}{cluster_name}{placeholder_end}", cluster_name
            )
            with open(filepath, "w") as file:
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
