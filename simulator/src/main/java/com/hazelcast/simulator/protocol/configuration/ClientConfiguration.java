package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.MessageFuture;
import com.hazelcast.simulator.protocol.core.Response;
import io.netty.channel.ChannelPipeline;

import java.util.concurrent.ConcurrentMap;

public interface ClientConfiguration {

    String getHost();

    int getPort();

    void configurePipeline(ChannelPipeline pipeline, ConcurrentMap<String, MessageFuture<Response>> futureMap);

    String createFutureKey(long messageId);
}
