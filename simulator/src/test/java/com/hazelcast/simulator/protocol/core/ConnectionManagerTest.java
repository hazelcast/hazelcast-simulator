package com.hazelcast.simulator.protocol.core;

import io.netty.channel.Channel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.Test;

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;

public class ConnectionManagerTest {

    private ConnectionManager connectionManager = new ConnectionManager();
    private Channel channel = new NioSocketChannel();

    @Test
    public void testConnectAndDisconnect() {
        assertEquals(0, connectionManager.size());

        connectionManager.connected(channel);
        assertEquals(1, connectionManager.size());

        connectionManager.disconnected(channel);
        assertEquals(0, connectionManager.size());
    }

    @Test(timeout = 5000)
    public void testWaitForAtLeastOneChannel() throws Exception {
        Thread addChannelThread = new Thread() {
            @Override
            public void run() {
                sleepMillis(500);
                try {
                    connectionManager.connected(channel);
                } catch (Exception e) {
                    throw rethrow(e);
                }
            }
        };
        addChannelThread.start();

        connectionManager.waitForAtLeastOneChannel();
        addChannelThread.join();
    }
}
