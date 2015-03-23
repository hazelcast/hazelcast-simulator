package com.hazelcast.simulator.common.messaging;

import com.hazelcast.simulator.utils.NativeUtils;

@MessageSpec(value = "blockHzTraffic", description = "configures iptables to block all incoming "
        + "traffic to TCP port range " + BlockTrafficMessage.PORTS
        + ". It requires sudo to be configured not ask for a password.")
public class BlockTrafficMessage extends RunnableMessage {
    static final String PORTS = "5700:5800";

    public BlockTrafficMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public void run() {
        String command = String.format("sudo /sbin/iptables -p tcp --dport %s -A INPUT -i eth0 -j REJECT", PORTS);
        NativeUtils.execute(command);
    }
}
