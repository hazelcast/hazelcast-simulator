package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.handler.MessageConsumeHandler;
import com.hazelcast.simulator.protocol.handler.MessageEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseHandler;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.handler.SimulatorProtocolDecoder;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import io.netty.channel.ChannelPipeline;

import java.util.concurrent.ConcurrentHashMap;

public class CoordinatorClientConfiguration extends AbstractClientConfiguration {

    private final OperationProcessor processor;
    private final SimulatorAddress localAddress;

    public CoordinatorClientConfiguration(OperationProcessor processor, int agentIndex, String agentHost, int agentPort) {
        super(new ConcurrentHashMap<String, ResponseFuture>(), SimulatorAddress.COORDINATOR, agentIndex, agentHost, agentPort);
        this.processor = processor;
        this.localAddress = SimulatorAddress.COORDINATOR;
    }

    @Override
    public void configurePipeline(ChannelPipeline pipeline) {
        pipeline.addLast("messageEncoder", new MessageEncoder(localAddress, getRemoteAddress()));
        pipeline.addLast("responseEncoder", new ResponseEncoder(localAddress));
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(localAddress));
        pipeline.addLast("responseHandler", new ResponseHandler(localAddress, getRemoteAddress(), getFutureMap()));
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(localAddress, processor));
    }
}
