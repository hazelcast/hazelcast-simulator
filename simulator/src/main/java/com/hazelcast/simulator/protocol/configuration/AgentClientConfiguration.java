package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.MessageFuture;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.handler.MessageEncoder;
import com.hazelcast.simulator.protocol.handler.ResponseHandler;
import com.hazelcast.simulator.protocol.handler.SimulatorFrameDecoder;
import com.hazelcast.simulator.protocol.handler.SimulatorProtocolDecoder;
import io.netty.channel.ChannelPipeline;

import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;

public class AgentClientConfiguration extends AbstractClientConfiguration {

    public AgentClientConfiguration(SimulatorAddress localAddress, int workerIndex, String host, int port) {
        super(localAddress, WORKER, workerIndex, host, port);
    }

    @Override
    public void configurePipeline(ChannelPipeline pipeline, ConcurrentMap<String, MessageFuture<Response>> futureMap) {
        pipeline.addLast("messageEncoder", new MessageEncoder(localAddress, addressLevel, addressIndex));
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("protocolDecoder", new SimulatorProtocolDecoder(localAddress, addressLevel));
        pipeline.addLast("responseHandler", new ResponseHandler(localAddress, addressLevel, addressIndex, futureMap));
    }
}
