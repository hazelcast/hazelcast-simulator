package com.hazelcast.simulator.common.messaging;

import com.hazelcast.simulator.utils.NativeUtils;
import org.apache.log4j.Logger;

@MessageSpec(value = "kill", description = "it causes process to kill itself by sending SIGKILL signal to itself.")
public class KillWorkerMessage extends RunnableMessage {
    private static final Logger LOGGER = Logger.getLogger(KillWorkerMessage.class);

    public KillWorkerMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public void run() {
        Integer pid = NativeUtils.getPIDorNull();
        LOGGER.info("I'm about to send kill -9 signal to myself. My PID is: " + pid);
        if (pid != null) {
            NativeUtils.kill(pid);
        }
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
