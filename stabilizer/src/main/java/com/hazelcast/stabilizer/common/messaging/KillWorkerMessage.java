package com.hazelcast.stabilizer.common.messaging;

import com.hazelcast.stabilizer.utils.NativeUtils;
import org.apache.log4j.Logger;

@MessageSpec(value = "kill", description = "it causes process to kill itself by sending SIGKILL signal to itself.")
public class KillWorkerMessage extends RunnableMessage {
    private static final Logger log = Logger.getLogger(KillWorkerMessage.class);

    public KillWorkerMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public void run() {
        Integer pid = NativeUtils.getPIDorNull();
        log.info("I'm about to send kill -9 signal to myself. My PID is: "+pid);
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
