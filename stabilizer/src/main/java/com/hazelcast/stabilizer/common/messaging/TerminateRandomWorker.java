package com.hazelcast.stabilizer.common.messaging;

/**
 * This message terminates a random worker when sent to an agent.
 * Termination is done by calling {@link Process#destroy()} from an agent,
 * resulting in sending a SIGTERM signal when running on Linux.
 *
 */
@MessageSpec("terminateWorker")
public class TerminateRandomWorker extends Message {

    public TerminateRandomWorker(MessageAddress messageAddress) {
        super(messageAddress);
    }
}
