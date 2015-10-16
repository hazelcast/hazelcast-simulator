package com.hazelcast.simulator.common.messaging;

import org.apache.log4j.Logger;

import static com.hazelcast.simulator.utils.NativeUtils.getPID;
import static com.hazelcast.simulator.utils.NativeUtils.kill;

@MessageSpec(value = "kill", description = "Instructs the receiving process to send a SIGKILL signal to itself.")
public class KillWorkerMessage extends RunnableMessage {

    private static final Logger LOGGER = Logger.getLogger(KillWorkerMessage.class);

    public KillWorkerMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public void run() {
        int pid = getPID();
        LOGGER.info("I'm about to send kill -9 signal to myself. My PID is: " + pid);
        if (pid > 0) {
            kill(pid);
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
