import yaml
import paramiko
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def load_yaml(file_path):
    with open(file_path, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(f"Error loading YAML file: {exc}")
            return None


def check_package_manager(ssh_client):
    stdin, stdout, stderr = ssh_client.exec_command("which yum")
    if stdout.read().decode().strip():
        return "yum"
    stdin, stdout, stderr = ssh_client.exec_command("which apt-get")
    if stdout.read().decode().strip():
        return "apt"
    return None


def apply_multi_latency(ssh_client, interface, target_ips, latency, band_number):
    stdin, stdout, stderr = ssh_client.exec_command("which tc")
    if not stdout.read().decode().strip():
        install_command = (
            "sudo yum install -y iproute-tc" if "yum" in check_package_manager(ssh_client)
            else "sudo apt-get update && sudo apt-get install -y iproute2"
        )
        ssh_client.exec_command(install_command)
        time.sleep(2)

    try:
        # Clear any existing qdiscs
        replace_command = f"sudo tc qdisc replace dev {interface} root handle 1: htb"
        ssh_client.exec_command(replace_command)

        # Create a class for the specific latency profile
        classid = f"1:{band_number}"
        print(f"Adding class {classid} with {latency}ms delay")
        class_command = f"sudo tc class add dev {interface} parent 1: classid {classid} htb rate 100mbit"
        ssh_client.exec_command(class_command)

        # Adjust latency for round-trip time if needed
        applied_latency_string = f"{latency}ms"
        print(f"Attaching netem to class {classid} with {applied_latency_string} latency.")
        netem_command = f"sudo tc qdisc add dev {interface} parent {classid} handle {band_number}0: netem delay {applied_latency_string}"
        ssh_client.exec_command(netem_command)

        # Apply u32 filters for each IP
        for ip in target_ips:
            print(f"Adding filter for {ip} with flowid {classid}")
            filter_command = f"sudo tc filter add dev {interface} protocol ip parent 1: prio 1 u32 match ip dst {ip}/32 flowid {classid}"
            ssh_client.exec_command(filter_command)
            filter_stderr = stderr.read().decode()
            if "Error" in filter_stderr:
                print(f"Error adding filter for {ip}: {filter_stderr}. Aborting filter setup for this IP.")

        # Verify the setup
        print(f"Verifying qdisc on {interface}")
        verify_qdisc_command = f"sudo tc qdisc show dev {interface}"
        stdin, stdout, stderr = ssh_client.exec_command(verify_qdisc_command)
        print(f"Qdisc verification: {stdout.read().decode()}")

        print(f"Verifying filters on {interface}")
        verify_filter_command = f"sudo tc filter show dev {interface}"
        stdin, stdout, stderr = ssh_client.exec_command(verify_filter_command)
        print(f"Filter verification: {stdout.read().decode()}")

        print("Finished applying latencies.")
    except Exception as e:
        print(f"An error occurred while applying latency: {str(e)}")


def apply_latency_global(ssh_client, interface, target_ips, latency, is_rtt):
    stdin, stdout, stderr = ssh_client.exec_command("which tc")
    if stdout.read().decode().strip() == "":
        ssh_client.exec_command("sudo yum install -y iproute-tc")
        time.sleep(2)

    # clear any existing qdiscs
    ssh_client.exec_command(f"sudo tc qdisc del dev {interface} root 2> /dev/null")
    # adjust latency for round-trip time if needed

    applied_latency = latency / 2 if is_rtt else latency

    # set up the root qdisc and netem
    commands = [
        f"sudo tc qdisc add dev {interface} root handle 1: prio",
        f"sudo tc qdisc add dev {interface} parent 1:3 handle 30: netem delay {applied_latency}ms"
    ]

    # execute the initial commands
    for command in commands:
        stdin, stdout, stderr = ssh_client.exec_command(command)
        stdout.channel.recv_exit_status()  # Ensure command completes
        print(f"Running: {command}")
        print(stdout.read().decode())
        print(stderr.read().decode())

    # apply the filter command for each IP individually
    for ip in target_ips:
        print(f"Adding latency for {ip}")
        filter_command = f"sudo tc filter add dev {interface} protocol ip parent 1:0 prio 1 u32 match ip dst {ip}/32 flowid 1:3"
        stdin, stdout, stderr = ssh_client.exec_command(filter_command)
        stdout.channel.recv_exit_status()
        print(f"Running: {filter_command}")
        print(stdout.read().decode())
        print(stderr.read().decode())


def configure_latencies(machine, all_hosts, target_latency, network_interface, rtt=False, latency_profiles=None):
    groupId, private_ip, public_ip, user, key, type = machine
    print(f"Configuring latencies for {public_ip}")

    target_ips = []
    if latency_profiles and 'relationships' in latency_profiles:
        for profile in latency_profiles['relationships']:
            if profile['source'] == groupId and profile['target'] != groupId:
                target_ips.append((profile['target'], profile['latency']))
                print(f"Adding relationship: {profile['source']} -> {profile['target']} with {profile['latency']}ms latency.")

        if not target_ips:
            print(f"No target IPs for {groupId}. Skipping latency configuration.")
            return

        # Apply the latency only if this machine is the source and has targets
        for idx, (target_group, latency) in enumerate(target_ips):
            ips = [m[1] for m in all_hosts if m[0] == target_group]
            print(f"Target IPs for group {target_group} from {groupId}: {ips}")
            if ips:
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                try:
                    print(f"Connecting to {public_ip} to apply latency.")
                    ssh_client.connect(public_ip, username=user, key_filename=key)

                    band_number = 10 + idx  # Use a unique band number for each relationship
                    print(f"Applying latency between {groupId} and {target_group}: {latency}ms with band {band_number}")
                    apply_multi_latency(ssh_client, network_interface, ips, latency, band_number)
                except Exception as e:
                    print(f"Error connecting to {public_ip}: {str(e)}")
                finally:
                    ssh_client.close()
    else:
        print(f"No latency profiles found. Using global latency per group.")
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        target_ips = []
        if type == 'client':
            # Clients should only add members in other data centers
            target_ips.extend(m[1] for m in all_hosts if m[0] != groupId and m[5] == 'member')
        elif type == 'member':
            # Members should add both clients and other members in different data centers
            target_ips.extend(m[1] for m in all_hosts if m[0] != groupId and m[5] == 'client')
            target_ips.extend(m[1] for m in all_hosts if m[0] != groupId and m[5] == 'member')
        try:
            ssh_client.connect(public_ip, username=user, key_filename=key)
            print(f"Applying fallback latency of {target_latency}ms.")
            apply_latency_global(ssh_client, network_interface, target_ips, target_latency, rtt)
        except Exception as e:
            print(f"Error connecting to {public_ip}: {str(e)}")
        finally:
            ssh_client.close()


def inject_latencies(target_latency, network_interface, rtt, profiles_path=None):
    yaml_file_path = 'inventory.yaml'
    config = load_yaml(yaml_file_path)
    latency_profiles = load_yaml(profiles_path) if profiles_path else None

    if latency_profiles and 'latency_profiles' in latency_profiles:
        latency_profiles = latency_profiles['latency_profiles']
        print(f"Loaded latency profiles from {profiles_path}.")
    else:
        print(f"No latency profiles found at {profiles_path}. Using default latency.")

    if config:
        all_hosts = []
        for group, host_type in [('loadgenerators', 'client'), ('nodes', 'member')]:
            hosts = config.get(group, {}).get('hosts', {})
            for public_ip, details in hosts.items():
                private_ip = details['private_ip']
                group = details.get('group', 'default')
                user = details['ansible_user']
                key = details['ansible_ssh_private_key_file']
                all_hosts.append((group, private_ip, public_ip, user, key, host_type))

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(configure_latencies, machine, all_hosts, target_latency, network_interface, rtt, latency_profiles)
                for machine in all_hosts
            ]
            for future in as_completed(futures):
                try:
                    future.result()  # If any thread throws an exception, it will be raised here
                except Exception as e:
                    print(f"Error occurred during latency configuration: {str(e)}")
