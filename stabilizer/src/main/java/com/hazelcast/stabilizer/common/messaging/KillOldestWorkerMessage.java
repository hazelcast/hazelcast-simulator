package com.hazelcast.stabilizer.common.messaging;

import com.hazelcast.stabilizer.NativeUtils;
import org.apache.log4j.Logger;

@MessageSpec("killOldest")
public class KillOldestWorkerMessage extends RunOnOldestWorkerMessage {
    private static final Logger log = Logger.getLogger(KillOldestWorkerMessage.class);

    public KillOldestWorkerMessage() {
        super(MessageAddress.builder().toAllAgents().toAllWorkers().build());
    }

    public KillOldestWorkerMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    protected void onOldest() {
        Integer pid = NativeUtils.getPIDorNull();
        log.info("I'm a worker with the oldest Hazelcast member. I'm about to send kill -9 signal to myself. " +
                "My PID is "+pid);
        if (pid != null) {
            NativeUtils.kill(pid);
        }
    }
}
