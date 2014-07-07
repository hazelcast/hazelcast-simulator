package com.hazelcast.stabilizer.common.messaging;

/**
 * This message terminates a random worker when sent to an agent.
 *
 */
@MessageSpec("terminateWorker")
public class TerminateRandomWorker extends Message {

    public TerminateRandomWorker(MessageAddress messageAddress) {
        super(messageAddress);
    }
}
