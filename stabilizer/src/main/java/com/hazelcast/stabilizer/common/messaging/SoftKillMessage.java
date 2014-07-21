package com.hazelcast.stabilizer.common.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@MessageSpec("softKill")
public class SoftKillMessage extends RunnableMessage {
    private static final Logger log = LoggerFactory.getLogger(SoftKillMessage.class);

    public SoftKillMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public void run() {
        log.warn("Processing soft kill message. I'm about to die!");
        System.exit(-1);
    }

    @Override
    public boolean disableMemberFailureDetection() {
        return true;
    }
}
