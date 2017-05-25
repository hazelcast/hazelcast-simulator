#!/bin/bash

# script that uploads the 'upload' directory.

# exit on failure
set -e

src_dir=$1
session_id=$2
# comma separated list of agent ip addresses
agents=$3
target_dir=hazelcast-simulator-$SIMULATOR_VERSION/workers/${session_id}

upload_remote(){
    agent=$1
    agent_index=$2

    echo "[INFO]    Upload [A$agent_index] $agent starting..."

    # if the local upload directory exist, it needs to be uploaded
    echo "Uploading upload directory $src_dir to $agent:$target_dir"
    ssh ${SSH_OPTIONS} ${SIMULATOR_USER}@${agent} "mkdir -p $target_dir"
    scp ${SSH_OPTIONS} -r ${src_dir} ${SIMULATOR_USER}@${agent}:${target_dir}

    echo "[INFO]    Upload [A$agent_index] $agent completed"
}

upload_local(){
    echo "[INFO]Upload local agent [A1] starting..."

    local_target_dir=${SIMULATOR_HOME}/workers/${session_id}

    mkdir -p $local_target_dir
    cp -rfv $src_dir $local_target_dir

    echo "[INFO]Upload local agent [A1] completed"
}

if [ ! -d ${src_dir} ]; then
    echo "[INFO]'$src_dir' directory doesn't exist, skipping upload."
    exit 0
fi

if [ "$CLOUD_PROVIDER" = "local" ]; then
    upload_local
else
    echo "[INFO]Upload 'upload' directory starting"
    agent_index=1
    for agent in ${agents//,/ } ; do
        upload_remote $agent $agent_index &
        ((agent_index++))
    done

    # todo: no feedback if the agent was actually started.
    wait
    echo "[INFO]Remote agents started"
fi


