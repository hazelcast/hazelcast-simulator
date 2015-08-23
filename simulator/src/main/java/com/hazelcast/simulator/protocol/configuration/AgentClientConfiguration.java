package com.hazelcast.simulator.protocol.configuration;

import com.hazelcast.simulator.protocol.core.MessageFuture;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.handler.MessageEncoder;
import com.hazelcast.simulator.protocol.handler.MessageResponseHandler;
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
        pipeline.addLast("frameDecoder", new SimulatorFrameDecoder());
        pipeline.addLast("decoder", new SimulatorProtocolDecoder(localAddress, addressLevel));
        pipeline.addLast("encoder", new MessageEncoder(localAddress, addressLevel, addressIndex));
        pipeline.addLast("handler", new MessageResponseHandler(localAddress, addressLevel, addressIndex,
                futureMap));
    }
}
