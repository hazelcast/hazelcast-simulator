package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.connector.ClientConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.MessageFuture;
import com.hazelcast.simulator.protocol.core.Response;
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

import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;

/**
 * Bootstrap configuration for a {@link com.hazelcast.simulator.protocol.connector.AgentConnector}.
 */
public class AgentServerConfiguration extends AbstractServerConfiguration {

    private final ChannelCollectorHandler channelCollectorHandler = new ChannelCollectorHandler();
    private final MessageForwardToWorkerHandler messageForwardToWorkerHandler = new MessageForwardToWorkerHandler(localAddress);

    private final OperationProcessor processor;

    public AgentServerConfiguration(SimulatorAddress localAddress, int addressIndex, int port, OperationProcessor processor) {
        super(localAddress, addressIndex, port);
        this.processor = processor;
    }

    @Override
    public AddressLevel getAddressLevel() {
        return AGENT;
    }

    @Override
    public void configurePipeline(ChannelPipeline pipeline, ConcurrentMap<String, MessageFuture<Response>> futureMap) {
        pipeline.addLast("responseEncoder", new ResponseEncoder(localAddress));
        pipeline.addLast("collector", channelCollectorHandler);
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(localAddress, AGENT));
        pipeline.addLast("messageForwardHandler", messageForwardToWorkerHandler);
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(localAddress, processor));
    }

    public void addWorker(int workerIndex, ClientConnector clientConnector) {
        messageForwardToWorkerHandler.addWorker(workerIndex, clientConnector);
    }

    public void removeWorker(int workerIndex) {
        messageForwardToWorkerHandler.removeWorker(workerIndex);
    }

    @Override
    public ChannelGroup getChannelGroup() {
        return channelCollectorHandler.getChannels();
    }
}
