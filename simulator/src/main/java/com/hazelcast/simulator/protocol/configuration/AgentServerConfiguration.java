package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.connector.ClientConnector;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.handler.ChannelCollectorHandler;
import com.hazelcast.simulator.protocol.handler.MessageConsumeHandler;
import com.hazelcast.simulator.protocol.handler.MessageForwardToWorkerHandler;
import com.hazelcast.simulator.protocol.handler.ResponseEncoder;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.handler.SimulatorProtocolDecoder;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;

import java.util.concurrent.ConcurrentMap;

/**
 * Bootstrap configuration for a {@link com.hazelcast.simulator.protocol.connector.AgentConnector}.
 */
public class AgentServerConfiguration extends AbstractServerConfiguration {

    private final ChannelCollectorHandler channelCollectorHandler = new ChannelCollectorHandler();
    private final MessageForwardToWorkerHandler messageForwardToWorkerHandler = new MessageForwardToWorkerHandler(localAddress);

    public AgentServerConfiguration(OperationProcessor processor, SimulatorAddress localAddress, int port) {
        super(processor, localAddress, port);
    }

    @Override
    public ChannelGroup getChannelGroup() {
        return channelCollectorHandler.getChannels();
    }

    @Override
    public void configurePipeline(ChannelPipeline pipeline, ConcurrentMap<String, ResponseFuture> futureMap) {
        pipeline.addLast("responseEncoder", new ResponseEncoder(localAddress));
        pipeline.addLast("collector", channelCollectorHandler);
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(localAddress));
        pipeline.addLast("messageForwardHandler", messageForwardToWorkerHandler);
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(localAddress, processor));
    }

    public void addWorker(int workerIndex, ClientConnector clientConnector) {
        messageForwardToWorkerHandler.addWorker(workerIndex, clientConnector);
    }

    public void removeWorker(int workerIndex) {
        messageForwardToWorkerHandler.removeWorker(workerIndex);
    }
}
