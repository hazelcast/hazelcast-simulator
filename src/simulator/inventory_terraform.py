import json
import os
from os import path
import subprocess

from simulator.util import shell, exit_with_error, read, write_yaml
from simulator.log import info


def terraform_apply(inventory_plan_yaml, force=False):
    terraform_plan = inventory_plan_yaml["terraform_plan"]

    __init(terraform_plan)
    __apply(terraform_plan, force)
    terraform_import(terraform_plan)


def __init(terraform_plan):
    if not path.exists(f"{terraform_plan}/.terraform"):
        cmd = f'terraform -chdir={terraform_plan} init'
        info(cmd)
        exitcode = shell(cmd)
        if exitcode != 0:
            exit_with_error(f'Failed terraform init, plan [{terraform_plan}], exitcode={exitcode} command=[{cmd}])')


def __apply(terraform_plan, force):
    info(f'Using terraform_plan [{terraform_plan}]')

    __ensure_plan_exists(terraform_plan)

    options = f'-auto-approve'

    cmd = f'terraform -chdir={terraform_plan} apply {options}'
    if force:
        cmd = f"{cmd} -lock=false"

    info(cmd)
    exitcode = shell(cmd)
    if exitcode != 0:
        exit_with_error(f'Failed terraform apply, plan [{terraform_plan}], exitcode={exitcode} command=[{cmd}])')


def terraform_import(terraform_plan):
    output = subprocess.check_output(f'terraform -chdir={terraform_plan} output -json', shell=True, text=True)

    inventory = {}

    for group_name, block in json.loads(output).items():
        json_host_list = block['value'][0]
        host_map = {}
        for json_host in json_host_list:
            host_name = json_host['public_ip']
            host_data = {'private_ip': json_host['private_ip']}
            tags = json_host.get("tags")

            if tags:
                for key, value in tags.items():
                    if key.startswith("passthrough:"):
                        host_data[key[len("passthrough:"):]] = value

            host_map[host_name] = host_data

        if host_map:
            hosts = {'hosts': host_map}
            inventory[group_name] = hosts

    info("Creating [inventory.yaml]")
    write_yaml("inventory.yaml", inventory)
    info(read("inventory.yaml"))


def terraform_destroy(inventory_plan_yaml, force=False):
    terraform_plan = inventory_plan_yaml["terraform_plan"]
    __destroy(terraform_plan, force)

    filename = "inventory.yaml"
    info(f"Removing [{filename}]")
    if os.path.exists(filename):
        os.remove(filename)


def __destroy(terraform_plan, force):
    info(f'Using terraform_plan [{terraform_plan}]')

    __ensure_plan_exists(terraform_plan)

    terraform_dir = f"{terraform_plan}/.terraform"
    if not os.path.exists(terraform_dir):
        return

    cmd = f'terraform -chdir={terraform_plan} destroy -auto-approve'
    if force:
        cmd = f"{cmd} -lock=false"

    info(cmd)
    exitcode = shell(cmd)
    if exitcode != 0:
        exit_with_error(f'Failed terraform destroy, plan [{terraform_plan}], exitcode={exitcode} command=[{cmd}])')


def __ensure_plan_exists(terraform_plan):
    if not path.isdir(terraform_plan):
        exit_with_error(f"Directory [{terraform_plan}] does not exist.")
