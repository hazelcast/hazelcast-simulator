#!/bin/bash

# Record timestamps when a benchmark is starting/stopping
# On each agent we want to know the exact time a benchmark is entering its 'running' phase and when it completes.
# This way we can filter out data e.g. from dstat, that is recording during preparation, verification etc.
# This is done by making a file

# exit on failure
set -e

# comma separated list of agent ip addresses
agents=$1
session_id=$2
test_id=$3
label=$4

record_timestamp_local(){
    echo $label=$(date +%s)>> ${SIMULATOR_HOME}/workers/${session_id}/A1_${test_id}.time
}

record_timestamp_remote(){
    agent=$1
    agent_index=$2
    cmd='$(date +%s)'
    ssh ${SSH_OPTIONS} ${SIMULATOR_USER}@${agent} \
        "echo $label=$cmd >> hazelcast-simulator-$SIMULATOR_VERSION/workers/${session_id}/A${agent_index}_${test_id}.time"
}

# Preparing session directory
if [ "$CLOUD_PROVIDER" = "local" ]; then
    record_timestamp_local
else
    agent_index=1
    for agent in ${agents//,/ } ; do
        record_timestamp_remote $agent $agent_index &
        ((agent_index++))
    done

    wait
fi
