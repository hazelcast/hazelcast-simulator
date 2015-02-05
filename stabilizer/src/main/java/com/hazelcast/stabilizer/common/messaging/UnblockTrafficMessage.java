package com.hazelcast.stabilizer.common.messaging;

import com.hazelcast.stabilizer.utils.NativeUtils;

@MessageSpec(value = "unblockTraffic", description = "unblock all traffic by calling 'sudo /sbin/iptables -F'")
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
