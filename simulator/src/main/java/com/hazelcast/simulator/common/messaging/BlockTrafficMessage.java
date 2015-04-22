package com.hazelcast.simulator.common.messaging;

import static com.hazelcast.simulator.utils.NativeUtils.execute;

@MessageSpec(value = "blockHzTraffic", description = "Configures iptables to block all incoming traffic to TCP port range "
        + BlockTrafficMessage.PORTS + ". Requires sudo to be configured not ask for a password.")
public class BlockTrafficMessage extends RunnableMessage {

    static final String PORTS = "5700:5800";

    public BlockTrafficMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public void run() {
        String command = String.format("sudo /sbin/iptables -p tcp --dport %s -A INPUT -i eth0 -j REJECT", PORTS);
        execute(command);
    }
}
