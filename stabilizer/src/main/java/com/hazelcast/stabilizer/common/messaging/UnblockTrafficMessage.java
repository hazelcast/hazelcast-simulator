package com.hazelcast.stabilizer.common.messaging;

import com.hazelcast.stabilizer.NativeUtils;

@MessageSpec("unblockTraffic")
public class UnblockTrafficMessage extends RunnableMessage {
    public UnblockTrafficMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public void run() {
        String command = "sudo /sbin/iptables -F";
        NativeUtils.execute(command);
    }
}
