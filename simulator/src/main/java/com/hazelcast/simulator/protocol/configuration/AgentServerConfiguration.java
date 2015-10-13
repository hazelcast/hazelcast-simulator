package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.connector.ClientConnector;
import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.handler.ChannelCollectorHandler;
import com.hazelcast.simulator.protocol.handler.ExceptionHandler;
import com.hazelcast.simulator.protocol.handler.ForwardToWorkerHandler;
import com.hazelcast.simulator.protocol.handler.MessageConsumeHandler;
import com.hazelcast.simulator.protocol.handler.MessageEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseHandler;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.handler.SimulatorProtocolDecoder;
import com.hazelcast.simulator.protocol.processors.AgentOperationProcessor;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;

import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;

/**
 * Bootstrap configuration for a {@link com.hazelcast.simulator.protocol.connector.AgentConnector}.
 */
public class AgentServerConfiguration extends AbstractServerConfiguration {

    private final SimulatorAddress localAddress;
    private final AgentOperationProcessor processor;

    private final ChannelCollectorHandler channelCollectorHandler;
    private final ForwardToWorkerHandler forwardToWorkerHandler;

    public AgentServerConfiguration(AgentOperationProcessor processor, ConcurrentMap<String, ResponseFuture> futureMap,
                                    SimulatorAddress localAddress, int port) {
        super(processor, futureMap, localAddress, port);
        this.localAddress = localAddress;
        this.processor = processor;

        this.channelCollectorHandler = new ChannelCollectorHandler();
        this.forwardToWorkerHandler = new ForwardToWorkerHandler(localAddress);
    }

    @Override
    public ChannelGroup getChannelGroup() {
        channelCollectorHandler.waitForAtLeastOneChannel();
        return channelCollectorHandler.getChannels();
    }

    @Override
    public void configurePipeline(ChannelPipeline pipeline, ServerConnector serverConnector) {
        pipeline.addLast("responseEncoder", new ResponseEncoder(localAddress));
        pipeline.addLast("messageEncoder", new MessageEncoder(localAddress, COORDINATOR));
        pipeline.addLast("collector", channelCollectorHandler);
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(localAddress));
        pipeline.addLast("forwardToWorkerHandler", forwardToWorkerHandler);
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(localAddress, processor));
        pipeline.addLast("responseHandler", new ResponseHandler(localAddress, COORDINATOR, getFutureMap(),
                getLocalAddressIndex()));
        pipeline.addLast("exceptionHandler", new ExceptionHandler(serverConnector));
    }

    public void addWorker(int workerIndex, ClientConnector clientConnector) {
        forwardToWorkerHandler.addWorker(workerIndex, clientConnector);
    }

    public void removeWorker(int workerIndex) {
        forwardToWorkerHandler.removeWorker(workerIndex);
    }

    public AgentClientConfiguration getClientConfiguration(int workerIndex, String workerHost, int workerPort,
                                                           AgentConnector agentConnector) {
        return new AgentClientConfiguration(agentConnector, processor, getFutureMap(), localAddress,
                workerIndex, workerHost, workerPort, channelCollectorHandler.getChannels());
    }
}
