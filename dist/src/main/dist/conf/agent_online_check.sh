#!/bin/bash

# script that checks if the agents can be connected to using ssh.

# exit on failure
set -e

# comma separated list of agent ip addresses
agents=$1

# joyful global variable containing the number of ssh test errors
ssh_test_errors=0

ssh_test(){
    agent=$1

    if [ "$agent" = "127.0.0.1" ]; then
        # we skip ssl check for local case
        return
    fi

    set +e
    ssh $SSH_OPTIONS -o ConnectTimeout=3 -q $SIMULATOR_USER@$agent exit
    status=$?
    set -e

    if [ "$status" -ne "0" ]; then
        ((ssh_test_errors+=1))
        echo "[ERROR]  Agent-machine $agent offline!"
    else
        echo "[INFO]    Agent-machine $agent online"
    fi
}

echo "[INFO]Checking agent-machine SSH reachable status..."

if [ "$CLOUD_PROVIDER" = "local" ]; then
    echo "[INFO]All agent-machines are SSH reachable."
    exit 0
fi

for agent in ${agents//,/ } ; do
    ssh_test $agent
done

if [ "$ssh_test_errors" -ne 0 ] ; then
    echo "[ERROR]Not all agent-machines are SSH reachable!"
    exit 1
fi

echo "[INFO]All agent-machines are SSH reachable."