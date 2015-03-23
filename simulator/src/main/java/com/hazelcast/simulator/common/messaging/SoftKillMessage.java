package com.hazelcast.simulator.common.messaging;


import org.apache.log4j.Logger;

@MessageSpec(value = "softKill", description = "instructs receiving party to call System.exit(-1)")
public class SoftKillMessage extends RunnableMessage {
    private static final Logger LOGGER = Logger.getLogger(SoftKillMessage.class);

    public SoftKillMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public void run() {
        LOGGER.warn("Processing soft kill message. I'm about to die!");
        System.exit(-1);
    }

    @Override
    public boolean removeFromAgentList() {
        return true;
    }

    @Override
    public boolean disableMemberFailureDetection() {
        return true;
    }
}
