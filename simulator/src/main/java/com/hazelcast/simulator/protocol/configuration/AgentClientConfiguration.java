package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.handler.MessageEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseHandler;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.handler.SimulatorProtocolDecoder;
import io.netty.channel.ChannelPipeline;

import java.util.concurrent.ConcurrentMap;

public class AgentClientConfiguration extends AbstractClientConfiguration {

    public AgentClientConfiguration(SimulatorAddress localAddress, int workerIndex, String workerHost, int workerPort) {
        super(localAddress, workerIndex, workerHost, workerPort);
    }

    @Override
    public void configurePipeline(ChannelPipeline pipeline, ConcurrentMap<String, ResponseFuture> futureMap) {
        pipeline.addLast("messageEncoder", new MessageEncoder(localAddress, remoteAddress));
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(localAddress));
        pipeline.addLast("responseHandler", new ResponseHandler(localAddress, remoteAddress, futureMap));
    }
}
