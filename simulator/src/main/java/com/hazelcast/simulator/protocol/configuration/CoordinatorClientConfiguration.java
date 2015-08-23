package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.MessageFuture;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.handler.MessageConsumeHandler;
import com.hazelcast.simulator.protocol.handler.MessageEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseHandler;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.handler.SimulatorProtocolDecoder;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import io.netty.channel.ChannelPipeline;

import java.util.concurrent.ConcurrentMap;

public class CoordinatorClientConfiguration extends AbstractClientConfiguration {

    private final OperationProcessor processor;

    public CoordinatorClientConfiguration(int agentIndex, String host, int port, OperationProcessor processor) {
        super(SimulatorAddress.COORDINATOR, agentIndex, host, port);
        this.processor = processor;
    }

    @Override
    public void configurePipeline(ChannelPipeline pipeline, ConcurrentMap<String, MessageFuture<Response>> futureMap) {
        pipeline.addLast("messageEncoder", new MessageEncoder(localAddress, targetAddress));
        pipeline.addLast("responseEncoder", new ResponseEncoder(localAddress));
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(localAddress));
        pipeline.addLast("responseHandler", new ResponseHandler(localAddress, targetAddress, futureMap));
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(localAddress, processor));
    }
}
