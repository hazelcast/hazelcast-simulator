#!/bin/bash

# exit on failure
set -e

# comma separated list of agent ip addresses
agents=$1

kill_agent_local(){
    echo "[INFO]Killing local agent"

    $SIMULATOR_HOME/bin/.kill-from-pid-file agent.pid

    rm agent.pid || true

    echo "[INFO]Killing local agent completed"
}

kill_agents_remote(){
    agent=$1

    echo "[INFO]    Killing agent ${agent}"

    ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "hazelcast-simulator-$SIMULATOR_VERSION/bin/.kill-from-pid-file agent.pid"
    ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "rm agent.pid || true"
}

kill_agents(){
    if [ "$CLOUD_PROVIDER" = "local" ]; then
        kill_agent_local
    else
        echo "[INFO]Killing remote agents..."

        for agent in ${agents//,/ } ; do
            kill_agents_remote $agent &
        done

        # wait for the tasks to complete
        wait

        echo "[INFO]Killing remote agents completed"
    fi
}

# no stopping required when embedded
if [ "$CLOUD_PROVIDER" = "embedded" ]; then
    exit 0
fi

kill_agents


