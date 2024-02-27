#!/usr/bin/env python3


from simulator.util import shell, run_parallel, simulator_home


def __upload_driver(host, agent_yaml, driver_dir):
    ssh_user = agent_yaml.get('ansible_user')
    ssh_key = agent_yaml.get('ansible_ssh_private_key_file')
    ssh_options = f"-i {ssh_key} -o StrictHostKeyChecking=no -o ConnectTimeout=60";

    print(f"[INFO]     {host}  Uploading")
    shell(
        f"""rsync --checksum -avv -L -e "ssh {ssh_options}" {simulator_home}/{driver_dir}/* {ssh_user}@{host}:hazelcast-simulator/{driver_dir}/""")
    print(f"[INFO]     {host}  Uploading: done")


def upload_driver(driver, agents_yaml):
    driver_dir = f"drivers/driver-{driver}"

    print(f"[INFO]Uploading driver {driver} to {driver_dir}: starting")
    run_parallel(__upload_driver, [(host, agent_yaml, driver_dir,) for host, agent_yaml in agents_yaml.items()])
    print(f"[INFO]Uploading driver {driver} to {driver_dir}: done")
