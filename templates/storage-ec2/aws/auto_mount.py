#!/usr/bin/python3

import subprocess
import threading

filesystem = "ext3"


def list_unmounted_partitions():
    cmd = 'lsblk --noheadings --raw -o NAME,MOUNTPOINT'
    lines = subprocess.check_output(cmd, shell=True, text=True).strip().splitlines()

    devices = []
    partitions = []

    for line in lines:
        record = line.split()
        name = record[0]

        if not name.startswith("nvme"):
            continue

        if "p" in name:
            partitions.append(name)
        elif len(record) == 1:
            devices.append(name)

    for dev in devices:
        for partition in partitions:
            if partition.startswith(dev):
                devices.remove(dev)
                break

    return devices


def format_and_mount(dev):
    cmd = f'sudo mkfs.{filesystem} /dev/{dev}'
    print(cmd)
    subprocess.run(cmd, shell=True, text=True, check=True)

    cmd = f'sudo mkdir -p /mnt/{dev}'
    print(cmd)
    subprocess.run(cmd, shell=True, text=True, check=True)

    cmd = f'sudo mount -t {filesystem} /dev/{dev} /mnt/{dev}'
    print(cmd)
    subprocess.run(cmd, shell=True, text=True, check=True)

    cmd = f'sudo chown ubuntu /mnt/{dev}'
    print(cmd)
    subprocess.run(cmd, shell=True, text=True, check=True)


unmounted = list_unmounted_partitions()

print(unmounted)

jobs = []
for dev in unmounted:
    job = threading.Thread(target=format_and_mount(dev))
    jobs.append(job)
    job.start()

for job in jobs:
    job.join()
