package com.hazelcast.simulator.common.messaging;

/**
 * This message terminates a random worker when sent to an agent.
 * Termination is done by calling {@link Process#destroy()} from an agent,
 * resulting in sending a SIGTERM signal when running on Linux.
 */
@MessageSpec(value = "terminateWorker", description = "indicates to an agent to terminate a random worker")
public class TerminateRandomWorkerMessage extends Message {

    public TerminateRandomWorkerMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public boolean disableMemberFailureDetection() {
        return super.disableMemberFailureDetection();
    }

    @Override
    public boolean removeFromAgentList() {
        return super.removeFromAgentList();
    }
}
