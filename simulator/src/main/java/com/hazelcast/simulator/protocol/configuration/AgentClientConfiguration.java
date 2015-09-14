package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.handler.ForwardToCoordinatorHandler;
import com.hazelcast.simulator.protocol.handler.MessageConsumeHandler;
import com.hazelcast.simulator.protocol.handler.MessageEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseHandler;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.handler.SimulatorProtocolDecoder;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;

import java.util.concurrent.ConcurrentMap;

public class AgentClientConfiguration extends AbstractClientConfiguration {

    private final SimulatorAddress localAddress;
    private final ChannelGroup channelGroup;

    private final MessageConsumeHandler messageConsumeHandler;

    public AgentClientConfiguration(OperationProcessor processor, ConcurrentMap<String, ResponseFuture> futureMap,
                                    SimulatorAddress localAddress, int workerIndex, String workerHost, int workerPort,
                                    ChannelGroup channelGroup) {
        super(futureMap, localAddress, workerIndex, workerHost, workerPort);
        this.localAddress = localAddress;
        this.channelGroup = channelGroup;

        this.messageConsumeHandler = new MessageConsumeHandler(localAddress, processor);
    }

    @Override
    public void configurePipeline(ChannelPipeline pipeline) {
        pipeline.addLast("responseEncoder", new ResponseEncoder(localAddress));
        pipeline.addLast("messageEncoder", new MessageEncoder(localAddress, getRemoteAddress()));
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(localAddress));
        pipeline.addLast("forwardToCoordinatorHandler", new ForwardToCoordinatorHandler(localAddress, channelGroup));
        pipeline.addLast("responseHandler", new ResponseHandler(localAddress, getRemoteAddress(), getFutureMap()));
        pipeline.addLast("messageConsumeHandler", messageConsumeHandler);
    }
}
