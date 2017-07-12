#!/bin/bash

# script prepares the 'session' directory. So in the 'workers' directory on the 'remote' Simulator installation, a directory
# is made with the session id, e.g. 2017-07-11__15_37_01. In this directory the 'upload' directory is copied if it exists.

# exit on failure
set -e

src_dir=$1
session_id=$2
# comma separated list of agent ip addresses
agents=$3
target_dir=hazelcast-simulator-$SIMULATOR_VERSION/workers/${session_id}

prepare_session_remote(){
    agent=$1
    agent_index=$2

    echo "[INFO]    Upload [A$agent_index] $agent starting..."

    # we remove the session directory first; in case of multiple executions with the same session-id
    ssh ${SSH_OPTIONS} ${SIMULATOR_USER}@${agent} "rm -fr $target_dir"

    # if the local upload directory exist, it needs to be uploaded
    echo "Uploading upload directory $src_dir to $agent:$target_dir"
    ssh ${SSH_OPTIONS} ${SIMULATOR_USER}@${agent} "mkdir -p $target_dir"
    scp ${SSH_OPTIONS} -r ${src_dir} ${SIMULATOR_USER}@${agent}:${target_dir}

    echo "[INFO]    Upload [A$agent_index] $agent completed"
}

prepare_session_local(){
    echo "[INFO]Upload local agent [A1] starting..."

    local_target_dir=${SIMULATOR_HOME}/workers/${session_id}

    # we remove the session directory first; in case of multiple executions with the same session-id
    rm -fr local_target_dir

    mkdir -p $local_target_dir
    cp -rfv $src_dir $local_target_dir

    echo "[INFO]Upload local agent [A1] completed"
}

if [ ! -d ${src_dir} ]; then
    echo "[INFO]'$src_dir' directory doesn't exist, skipping upload."
    exit 0
fi

if [ "$CLOUD_PROVIDER" = "local" ]; then
    prepare_session_local
else
    echo "[INFO]Upload 'upload' directory starting..."
    agent_index=1
    for agent in ${agents//,/ } ; do
        prepare_session_remote $agent $agent_index &
        ((agent_index++))
    done

    wait
    echo "[INFO]Upload completed"
fi


