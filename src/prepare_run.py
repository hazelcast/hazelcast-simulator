#!/usr/bin/env python3
import os.path
import sys
import yaml

from simulator.hosts import public_ip, ssh_user, ssh_options, agent_index
from simulator.ssh import Ssh
from simulator.util import run_parallel


def prepare_run_dir(agent):
    ssh = Ssh(public_ip(agent), ssh_user(agent), ssh_options(agent))
    ssh.exec(f"""
        rm -fr {target_dir}
        mkdir -p {target_dir}
        """)


def upload(agent):
    ssh = Ssh(public_ip(agent), ssh_user(agent), ssh_options(agent))
    ssh.scp_to_remote(upload_dir, target_dir)


def start_dstat(agent):
    ssh = Ssh(public_ip(agent), ssh_user(agent), ssh_options(agent))
    ssh.exec(f"""
            set -e
            killall -9 dstat || true
            nohup dstat --epoch -m --all -l --noheaders --nocolor --output {target_dir}/A{agent_index(agent)}_dstat.csv 1 > /dev/null 2>&1 &
            sleep 1
            """)


upload_dir = sys.argv[1]
run_id = sys.argv[2]
agents_yaml = yaml.safe_load(sys.argv[3])
target_dir = f"hazelcast-simulator/workers/{run_id}"

run_parallel(prepare_run_dir, [(agent,) for agent in agents_yaml])

if os.path.exists(upload_dir):
    run_parallel(upload, [(agent,) for agent in agents_yaml])

run_parallel(start_dstat, [(agent,) for agent in agents_yaml])

