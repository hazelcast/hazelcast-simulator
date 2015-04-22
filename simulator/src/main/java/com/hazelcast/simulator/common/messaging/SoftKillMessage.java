package com.hazelcast.simulator.common.messaging;


import org.apache.log4j.Logger;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;

@MessageSpec(value = "softKill", description = "Instructs receiving party to call System.exit(-1).")
public class SoftKillMessage extends RunnableMessage {

    private static final Logger LOGGER = Logger.getLogger(SoftKillMessage.class);

    public SoftKillMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public void run() {
        LOGGER.warn("Processing soft kill message. I'm about to die!");
        exitWithError();
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
