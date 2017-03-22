#!/bin/bash

# exit on failure
set -e

# comma separated list of agent ip addresses
agents=$1

verify_installation(){
    if [ "$CLOUD_PROVIDER" != "local" ]; then
        for agent in ${agents//,/ } ; do
            status=$(ssh $SSH_OPTIONS $SIMULATOR_USER@$agent \
                "[[ -f hazelcast-simulator-$SIMULATOR_VERSION/bin/agent ]] && echo OK || echo FAIL")

             if [ $status != "OK" ] ; then
                echo "[ERROR]Simulator is not installed correctly on $agent. Please run provisioner --install to fix this."
                exit 1
            fi
        done
    fi
}

start_remote(){
    agent=$1
    agent_index=$2

    echo "[INFO]Agent [A$agent_index] $agent starting"

    ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "killall -9 java || true"
    ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "rm -f agent.pid"
    ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "rm -f agent.out"
    ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "rm -f agent.err"

    args="--addressIndex $agent_index --publicAddress $agent --port $AGENT_PORT"

    ssh $SSH_OPTIONS $SIMULATOR_USER@$agent \
        "nohup hazelcast-simulator-$SIMULATOR_VERSION/bin/agent $args > agent.out 2> agent.err < /dev/null &"

    ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "hazelcast-simulator-$SIMULATOR_VERSION/bin/.await-file-exists agent.pid"

    echo "[INFO]Agent [A$agent_index] $agent started successfully"
}

start_local(){
    echo "[INFO]Local agent [A1] starting"

    if [ -f agent.pid ]; then
        $SIMULATOR_HOME/bin/.kill-from-pid-file agent.pid
        rm agent.pid || true
    fi

    rm agent.out || true
    rm agent.err || true

    args="--addressIndex 1 --publicAddress 127.0.0.1 --port $AGENT_PORT"

    nohup $SIMULATOR_HOME/bin/agent $args > agent.out 2> agent.err < /dev/null &

    $SIMULATOR_HOME/bin/.await-file-exists agent.pid

    echo "[INFO]Local agent [A1] started"
}

start(){
    if [ "$CLOUD_PROVIDER" = "local" ]; then
        start_local
    else
        echo "[INFO]Remote agents starting"
        agent_index=1
        for agent in ${agents//,/ } ; do
            start_remote $agent $agent_index &
            ((agent_index++))
        done

        # todo: no feedback if the agent was actually started.
        wait
        echo "[INFO]Remote agents started"
    fi
}

# no starting required when embedded
if [ "$CLOUD_PROVIDER" = "embedded" ]; then
    exit 0
fi

verify_installation
start