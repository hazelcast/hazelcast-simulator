import json
import os
from os import path
import subprocess
import boto3
import re

from simulator.util import shell, exit_with_error, read_file, write_yaml
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
        if 'value' in block and block['value']:  # Check if 'value' key exists and is not empty
            json_host_list = block['value'][0]
            host_map = {}
            for json_host in json_host_list:
                if group_name == 'load_balancers':
                    host_name = json_host['dns_name']
                    host_data = {'arn': json_host['arn']}
                    private_ip = __get_nlb_private_ip(json_host)
                    host_data['private_ip'] = private_ip

                else:
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
    info(read_file("inventory.yaml"))


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


def extract_aws_region_from_arn(arn):
    # ARN format: arn:partition:service:region:account-id:resource
    match = re.match(r'arn:[^:]+:[^:]+:([^:]+):', arn)
    if match:
        return match.group(1)
    return "default_region"


def extract_aws_region_from_arn(arn):
    # ARN format: arn:aws:elasticloadbalancing:<region>:<account-id>:loadbalancer/<lb-name>/<lb-id>
    match = re.match(r'arn:aws:elasticloadbalancing:([^:]+):', arn)
    if match:
        return match.group(1)
    else:
        return None


def extract_lb_name_from_arn(arn):
    try:
        parts = arn.split('/')
        index = parts.index('net')
        return parts[index + 1] if index + 1 < len(parts) else None
    except Exception as e:
        print(f"Error extracting NLB name from ARN: {e}")
        return None


def __get_nlb_private_ip(data):
    aws_region = extract_aws_region_from_arn(data['arn'])
    aws_nlb_name = extract_lb_name_from_arn(data['arn'])

    if aws_region is None or aws_nlb_name is None:
        print("Error: Unable to extract AWS region or NLB name from ARN.")
        return None

    ec2 = boto3.client('ec2', region_name=aws_region)
    response = ec2.describe_network_interfaces(
        Filters=[
            {
                'Name': 'description',
                'Values': [
                    f"ELB net/{aws_nlb_name}/*"
                ]
            },
            {
                'Name': 'vpc-id',
                'Values': [
                    data['vpc_id']
                ]
            },
            {
                'Name': 'status',
                'Values': [
                    "in-use"
                ]
            },
            {
                'Name': 'attachment.status',
                'Values': [
                    "attached"
                ]
            }
        ]
    )
    interfaces = response['NetworkInterfaces']

    associations = [
        association for interface in interfaces
        for association in interface['PrivateIpAddresses']
    ]

    if associations:
        return associations[0]['PrivateIpAddress']
    else:
        print("Error: No private IP found.")
        return None

