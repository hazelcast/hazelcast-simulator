package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.connector.ClientConnector;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.handler.ChannelCollectorHandler;
import com.hazelcast.simulator.protocol.handler.ForwardToWorkerHandler;
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

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;

/**
 * Bootstrap configuration for a {@link com.hazelcast.simulator.protocol.connector.AgentConnector}.
 */
public class AgentServerConfiguration extends AbstractServerConfiguration {

    private final ChannelCollectorHandler channelCollectorHandler = new ChannelCollectorHandler();
    private final ForwardToWorkerHandler forwardToWorkerHandler = new ForwardToWorkerHandler(localAddress);

    public AgentServerConfiguration(OperationProcessor processor, ConcurrentMap<String, ResponseFuture> futureMap,
                                    SimulatorAddress localAddress, int port) {
        super(processor, futureMap, localAddress, port);
    }

    @Override
    public ChannelGroup getChannelGroup() {
        return channelCollectorHandler.getChannels();
    }

    @Override
    public void configurePipeline(ChannelPipeline pipeline) {
        pipeline.addLast("responseEncoder", new ResponseEncoder(localAddress));
        pipeline.addLast("messageEncoder", new MessageEncoder(localAddress, COORDINATOR));
        pipeline.addLast("collector", channelCollectorHandler);
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(localAddress));
        pipeline.addLast("forwardToWorkerHandler", forwardToWorkerHandler);
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(localAddress, processor));
        pipeline.addLast("responseHandler", new ResponseHandler(localAddress, COORDINATOR, futureMap, addressIndex));
    }

    public void addWorker(int workerIndex, ClientConnector clientConnector) {
        forwardToWorkerHandler.addWorker(workerIndex, clientConnector);
    }

    public void removeWorker(int workerIndex) {
        forwardToWorkerHandler.removeWorker(workerIndex);
    }

    public AgentClientConfiguration getClientConfiguration(int workerIndex, String workerHost, int workerPort) {
        return new AgentClientConfiguration(processor, futureMap, localAddress, workerIndex, workerHost, workerPort, getChannelGroup());
    }
}
