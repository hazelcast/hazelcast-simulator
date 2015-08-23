package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.connector.ClientConnector;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.handler.ChannelCollectorHandler;
import com.hazelcast.simulator.protocol.handler.MessageConsumeHandler;
import com.hazelcast.simulator.protocol.handler.MessageEncoder;
import com.hazelcast.simulator.protocol.handler.MessageForwardToWorkerHandler;
import com.hazelcast.simulator.protocol.handler.ResponseEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseHandler;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.handler.SimulatorProtocolDecoder;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;

import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;

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
        pipeline.addLast("messageEncoder", new MessageEncoder(localAddress, COORDINATOR));
        pipeline.addLast("collector", channelCollectorHandler);
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(localAddress));
        pipeline.addLast("messageForwardHandler", messageForwardToWorkerHandler);
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(localAddress, processor));
        pipeline.addLast("responseHandler", new ResponseHandler(localAddress, COORDINATOR, futureMap, addressIndex));
    }

    public void addWorker(int workerIndex, ClientConnector clientConnector) {
        messageForwardToWorkerHandler.addWorker(workerIndex, clientConnector);
    }

    public void removeWorker(int workerIndex) {
        messageForwardToWorkerHandler.removeWorker(workerIndex);
    }
}
