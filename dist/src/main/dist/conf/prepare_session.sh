#!/bin/bash

# script prepares the 'session'. So in the 'workers' directory on the 'remote' Simulator installation, a directory
# is made with the session id, e.g. 2017-07-11__15_37_01.
# In this directory the 'upload' directory is copied if it exists.
# Also dstat is started if available so we get dstat data for the benchmarking report.

# exit on failure
set -e

src_dir=$1
session_id=$2
# comma separated list of agent ip addresses
agents=$3
target_dir=hazelcast-simulator-$SIMULATOR_VERSION/workers/${session_id}

prepare_directory_local(){
    # we remove the session directory first; in case of multiple executions with the same session-id
    rm -fr ${SIMULATOR_HOME}/workers/${session_id}
    mkdir -p ${SIMULATOR_HOME}/workers/${session_id}
}

prepare_directory_remote(){
    agent=$1
    agent_index=$2
    # we remove the session directory first; in case of multiple executions with the same session-id
    ssh ${SSH_OPTIONS} ${SIMULATOR_USER}@${agent} "rm -fr $target_dir"
    ssh ${SSH_OPTIONS} ${SIMULATOR_USER}@${agent} "mkdir -p $target_dir"
}

prepare_directory(){
    # Preparing session directory
    if [ "$CLOUD_PROVIDER" = "local" ]; then
        prepare_directory_local
    else
        agent_index=1
        for agent in ${agents//,/ } ; do
            prepare_directory_remote $agent $agent_index &
            ((agent_index++))
        done

        wait
   fi
}

upload_remote(){
    agent=$1
    agent_index=$2

    echo "[INFO]    Upload [A$agent_index] $agent starting..."
    # if the local upload directory exist, it needs to be uploaded
    echo "Uploading upload directory $src_dir to $agent:$target_dir"
    scp ${SSH_OPTIONS} -r ${src_dir} ${SIMULATOR_USER}@${agent}:${target_dir}
    echo "[INFO]    Upload [A$agent_index] $agent completed"
}

upload_local(){
    echo "[INFO]Upload local agent [A1] starting..."
    cp -rfv $src_dir ${SIMULATOR_HOME}/workers/${session_id}
    echo "[INFO]Upload local agent [A1] completed"
}

upload(){
    # Uploading 'upload' directory if exist
    if [ ! -d ${src_dir} ]; then
        echo "[INFO]'$src_dir' directory doesn't exist, skipping upload."
    else
        if [ "$CLOUD_PROVIDER" = "local" ]; then
            upload_local
        else
            echo "[INFO]Upload 'upload' directory starting..."
            agent_index=1
            for agent in ${agents//,/ } ; do
                upload_remote $agent $agent_index &
                ((agent_index++))
            done

            wait
            echo "[INFO]Upload completed"
        fi
    fi
}

start_dstat_local(){
    # kill any dstat instances that are still running
    killall -9 dstat || true

    dstat --epoch -m --all -l --noheaders --nocolor --output ${SIMULATOR_HOME}/workers/${session_id}/A1_dstat.csv 5 > /dev/null &
}

start_dstat_remote(){
    agent=$1
    agent_index=$2

    # kill any dstat instances that are still running
    ssh ${SSH_OPTIONS} ${SIMULATOR_USER}@${agent} " killall -9 dstat || true"

    ssh ${SSH_OPTIONS} ${SIMULATOR_USER}@${agent} \
        "nohup dstat --epoch -m --all -l --noheaders --nocolor --output $target_dir/A${agent_index}_dstat.csv 5 > /dev/null &"
}

start_dstat(){
    # Starting dstat
    if [ "$CLOUD_PROVIDER" = "local" ]; then
        start_dstat_local
    else
        agent_index=1
        for agent in ${agents//,/ } ; do
            start_dstat_remote $agent $agent_index &
            ((agent_index++))
        done
        wait
    fi
}

prepare_directory
upload
start_dstat
