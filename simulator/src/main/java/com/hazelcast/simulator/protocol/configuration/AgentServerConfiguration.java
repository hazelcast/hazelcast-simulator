package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.connector.ClientConnector;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.handler.MessageConsumeHandler;
import com.hazelcast.simulator.protocol.handler.MessageDecoder;
import com.hazelcast.simulator.protocol.handler.MessageForwardToWorkerHandler;
import com.hazelcast.simulator.protocol.handler.ResponseEncoder;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.processors.OperationProcessor;
import io.netty.channel.ChannelPipeline;

import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;

/**
 * Bootstrap configuration for a {@link com.hazelcast.simulator.protocol.connector.AgentConnector}.
 */
public class AgentServerConfiguration extends AbstractBootstrapConfiguration {

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
    public void configurePipeline(ChannelPipeline pipeline) {
        pipeline.addLast("encoder", new ResponseEncoder());
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("decoder", new MessageDecoder(localAddress, AGENT));
        pipeline.addLast("forwarder", messageForwardToWorkerHandler);
        pipeline.addLast("consumer", new MessageConsumeHandler(localAddress, processor));
    }

    public void addWorker(int workerIndex, ClientConnector clientConnector) {
        messageForwardToWorkerHandler.addWorker(workerIndex, clientConnector);
    }

    public void removeWorker(int workerIndex) {
        messageForwardToWorkerHandler.removeWorker(workerIndex);
    }
}
