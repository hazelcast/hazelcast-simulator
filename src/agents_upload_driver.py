#!/usr/bin/env python3

import yaml
import sys
from simulator.util import shell, run_parallel, simulator_home
from simulator.hosts import public_ip, ssh_user, ssh_options


def __upload_driver(agent):
    print(f"[INFO]     {public_ip(agent)}  Uploading")
    shell(
        f"""rsync --checksum -avv -L -e "ssh {ssh_options(agent)}" {simulator_home}/{driver_dir}/* {ssh_user(agent)}@{public_ip(agent)}:hazelcast-simulator/{driver_dir}/""")
    print(f"[INFO]     {public_ip(agent)}  Uploading: done")


agents_yaml = yaml.safe_load(sys.argv[1])
driver = sys.argv[2]

driver_dir = f"drivers/driver-{driver}"

print(f"[INFO]Uploading driver {driver} to {driver_dir}: starting")
run_parallel(__upload_driver, [(agent,) for agent in agents_yaml])
print(f"[INFO]Uploading driver {driver} to {driver_dir}: done")
