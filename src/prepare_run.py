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


# #!/bin/bash
#
# # script prepares the 'session'.
# # 1: create a directory in 'workers' on the 'remote' Simulator installation e.g. 2017-07-11__15_37_01.
# # 2: copy the 'upload' directory is copied if it exists.
# # 3: start dstat if available so we get dstat data for the benchmarking report.
#
# # exit on failure
# set -e
#
# src_dir=$1
# session_id=$2
# # comma separated list of agent ip addresses
# agents=$3
# target_dir=hazelcast-simulator-$SIMULATOR_VERSION/workers/${session_id}


# upload_remote(){
#     agent=$1
#     agent_index=$2
#
#     echo "[INFO]    Upload [A$agent_index] $agent starting..."
#     # if the local upload directory exist, it needs to be uploaded
#     echo "Uploading upload directory $src_dir to $agent:$target_dir"
#     scp ${SCP_OPTIONS} -r ${src_dir} ${SIMULATOR_USER}@${agent}:${target_dir}
#     echo "[INFO]    Upload [A$agent_index] $agent completed"
# }
#
# upload(){
#     # Uploading 'upload' directory if exist
#     if [ ! -d ${src_dir} ]; then
#         echo "[DEBUG]'$src_dir' directory doesn't exist, skipping upload."
#     else
#         echo "[INFO]Upload 'upload' directory starting..."
#         agent_index=1
#         for agent in ${agents//,/ } ; do
#             upload_remote $agent $agent_index &
#             ((agent_index++))
#         done
#
#         wait
#         echo "[INFO]Upload completed"
#     fi
# }
# start_dstat_remote(){
#     agent=$1
#     agent_index=$2
#
#     # kill any dstat instances that are still running
#     ssh ${SSH_OPTIONS} ${SIMULATOR_USER}@${agent} " killall -9 dstat || true"
#
#     ssh -n ${SSH_OPTIONS} ${SIMULATOR_USER}@${agent} \
#         "nohup dstat --epoch -m --all -l --noheaders --nocolor --output $target_dir/A${agent_index}_dstat.csv 5 > /dev/null 2>&1 &"
# }
#
# start_dstat(){
#     agent_index=1
#     for agent in ${agents//,/ } ; do
#         start_dstat_remote $agent $agent_index &
#         ((agent_index++))
#     done
#     wait
# }
#
# prepare_run_dir
# upload
# start_dstat
