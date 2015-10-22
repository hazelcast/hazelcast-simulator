package com.hazelcast.simulator.protocol.core;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroup;
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
        ChannelGroup channelGroup = connectionManager.getChannels();
        assertEquals(0, channelGroup.size());

        connectionManager.connected(channel);
        assertEquals(1, channelGroup.size());

        connectionManager.disconnected(channel);
        assertEquals(0, channelGroup.size());
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

    @Test(timeout = 5000)
    public void testWaitForAtLeastOneChannel_interrupted() throws Exception {
        final Thread currentThread = Thread.currentThread();
        Thread interruptThread = new Thread() {
            @Override
            public void run() {
                sleepMillis(500);
                currentThread.interrupt();
            }
        };
        interruptThread.start();

        connectionManager.waitForAtLeastOneChannel();
        interruptThread.join();
    }
}
