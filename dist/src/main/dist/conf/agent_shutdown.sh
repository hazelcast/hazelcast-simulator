#!/bin/bash

# exit on failure
set -e

# comma separated list of agent ip addresses
agents=$1

kill_agent_local(){
    echo "[INFO]Killing local agent"

    $SIMULATOR_HOME/bin/.kill-from-pid-file agent.pid

    rm agent.pid || true

    echo "[INFO]Killing local completed"
}

kill_agents_remote(){
    agent=$1

    echo "[INFO]Killing agent ${agent}"

    ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "hazelcast-simulator-$SIMULATOR_VERSION/bin/.kill-from-pid-file agent.pid"
    ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "rm agent.pid || true"
}

kill_agents(){
    if [ "$CLOUD_PROVIDER" = "local" ]; then
        kill_agent_local
    else
        echo "[INFO]Killing remote agents"

        for agent in ${agents//,/ } ; do
            kill_agents_remote $agent &
        done

        # wait for the tasks to complete
        wait

        echo "[INFO]Killing remote agents completed"
    fi
}

start_harakiri_monitor(){
    # we install the harakiri monitor on ec2 when it is enabled to make sure we don't run into a big bill

    if [ "$CLOUD_PROVIDER" = "aws-ec2" ] && [ "$HARAKIRI_MONITOR_ENABLED" = "true" ] ; then
        echo "[INFO]Starting Harakiri monitor. In $HARAKIRI_MONITOR_WAIT_SECONDS seconds the machines will kill themselves"
        for agent in ${agents//,/ } ; do
            ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "nohup hazelcast-simulator-$SIMULATOR_VERSION/bin/harakiri-monitor \
                    --cloudProvider $CLOUD_PROVIDER --cloudIdentity $CLOUD_IDENTITY --cloudCredential $CLOUD_CREDENTIAL \
                    --waitSeconds $HARAKIRI_MONITOR_WAIT_SECONDS > harakiri.out 2> harakiri.err < /dev/null &"
        done
    fi
}

# no stopping required when embedded
if [ "$CLOUD_PROVIDER" = "embedded" ]; then
    exit 0
fi

start_harakiri_monitor
kill_agents


