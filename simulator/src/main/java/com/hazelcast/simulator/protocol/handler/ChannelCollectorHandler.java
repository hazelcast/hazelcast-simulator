package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.utils.EmptyStatement;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.util.concurrent.CountDownLatch;

/**
 * Collects active channels in a {@link ChannelGroup}, so they can be used to send messages to them.
 */
public class ChannelCollectorHandler extends ChannelInboundHandlerAdapter {

    private final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
        Channel channel = ctx.channel();
        channels.add(channel);
        countDownLatch.countDown();
    }

    public void waitForAtLeastOneChannel() {
        try {
            countDownLatch.await();
        } catch (InterruptedException ignored) {
            EmptyStatement.ignore(ignored);
        }
    }

    public ChannelGroup getChannels() {
        return channels;
    }
}
