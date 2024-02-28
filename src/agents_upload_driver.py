#!/usr/bin/env python3


from simulator.util import shell, run_parallel, simulator_home


def __upload_driver(host, driver_dir):
    print(f"[INFO]     {host['public_ip']}  Uploading")
    shell(
        f"""rsync --checksum -avv -L -e "ssh {host['ssh_options']}" {simulator_home}/{driver_dir}/* {host['ssh_user']}@{host['public_ip']}:hazelcast-simulator/{driver_dir}/""")
    print(f"[INFO]     {host['public_ip']}  Uploading: done")


def upload_driver(driver, hosts):
    driver_dir = f"drivers/driver-{driver}"

    print(f"[INFO]Uploading driver {driver} to {driver_dir}: starting")
    run_parallel(__upload_driver, [(host, driver_dir,) for host in hosts])
    print(f"[INFO]Uploading driver {driver} to {driver_dir}: done")
