package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmManager;
import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.core.ConnectionManager;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.handler.ExceptionHandler;
import com.hazelcast.simulator.protocol.handler.ForwardToCoordinatorHandler;
import com.hazelcast.simulator.protocol.handler.MessageConsumeHandler;
import com.hazelcast.simulator.protocol.handler.MessageEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseHandler;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.handler.SimulatorProtocolDecoder;
import com.hazelcast.simulator.protocol.processors.AgentOperationProcessor;
import io.netty.channel.ChannelPipeline;

import java.util.concurrent.ConcurrentMap;

public class AgentClientConfiguration extends AbstractClientConfiguration {

    private final AgentConnector agentConnector;
    private final ConnectionManager connectionManager;
    private final WorkerJvmManager workerJvmManager;
    private final AgentOperationProcessor processor;
    private final SimulatorAddress localAddress;

    public AgentClientConfiguration(AgentConnector agentConnector, ConnectionManager connectionManager,
                                    WorkerJvmManager workerJvmManager, AgentOperationProcessor processor,
                                    ConcurrentMap<String, ResponseFuture> futureMap, SimulatorAddress localAddress,
                                    int workerIndex, String workerHost, int workerPort) {
        super(futureMap, localAddress, workerIndex, workerHost, workerPort);
        this.agentConnector = agentConnector;
        this.connectionManager = connectionManager;
        this.workerJvmManager = workerJvmManager;
        this.processor = processor;
        this.localAddress = localAddress;
    }

    @Override
    public void configurePipeline(ChannelPipeline pipeline) {
        pipeline.addLast("responseEncoder", new ResponseEncoder(localAddress));
        pipeline.addLast("messageEncoder", new MessageEncoder(localAddress, getRemoteAddress()));
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(localAddress, workerJvmManager));
        pipeline.addLast("forwardToCoordinatorHandler", new ForwardToCoordinatorHandler(localAddress, connectionManager,
                workerJvmManager));
        pipeline.addLast("responseHandler", new ResponseHandler(localAddress, getRemoteAddress(), getFutureMap()));
        pipeline.addLast("messageConsumeHandler", new MessageConsumeHandler(localAddress, processor));
        pipeline.addLast("exceptionHandler", new ExceptionHandler(agentConnector));
    }
}
