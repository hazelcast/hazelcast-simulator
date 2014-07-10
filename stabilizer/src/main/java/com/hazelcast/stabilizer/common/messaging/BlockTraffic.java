package com.hazelcast.stabilizer.common.messaging;

import com.hazelcast.stabilizer.NativeUtils;
import com.hazelcast.stabilizer.agent.remoting.AgentRemoteService;

import java.io.Serializable;

/**
 *
 *
 */
@MessageSpec("blockHzTraffic")
public class BlockTraffic extends RunnableMessage {
    private static final String ports = "5700:5800";

    public BlockTraffic(MessageAddress messageAddress) {
        super(messageAddress);
    }

    @Override
    public void run() {
        String command = String.format("sudo /sbin/iptables -p tcp --dport %s -A INPUT -i eth0 -j REJECT", ports);
        NativeUtils.execute(command);
    }

}
