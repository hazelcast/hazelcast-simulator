package com.hazelcast.simulator.common.messaging;

import static com.hazelcast.simulator.utils.NativeUtils.execute;

@MessageSpec(value = "unblockTraffic", description = "Unblocks all traffic by calling 'sudo /sbin/iptables -F'.")
public class UnblockTrafficMessage extends RunnableMessage {

    public UnblockTrafficMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public void run() {
        String command = "sudo /sbin/iptables -F";
        execute(command);
    }
}
