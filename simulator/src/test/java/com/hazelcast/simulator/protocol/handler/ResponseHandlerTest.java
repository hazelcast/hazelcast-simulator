package com.hazelcast.simulator.protocol.handler;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.protocol.core.ResponseFuture.createFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createInstance;
import static org.junit.Assert.assertEquals;

public class ResponseHandlerTest {

    private final SimulatorAddress localAddress = SimulatorAddress.COORDINATOR;
    private final SimulatorAddress remoteAddress = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);

    private ConcurrentMap<String, ResponseFuture> futureMap;

    private ResponseHandler responseHandler;

    @Before
    public void setUp() {
        futureMap = new ConcurrentHashMap<String, ResponseFuture>();

        responseHandler = new ResponseHandler(localAddress, remoteAddress, futureMap);
    }

    @Test
    public void testChannelRead0() throws Exception {
        long messageId = 2948;
        Response response = new Response(messageId, remoteAddress);

        String futureKey = createFutureKey(response.getDestination(), messageId, remoteAddress.getAddressIndex());
        ResponseFuture responseFuture = createInstance(futureMap, futureKey);

        responseHandler.channelRead0(null, response);

        assertEquals(response, responseFuture.get(1, TimeUnit.SECONDS));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testChannelRead0_futureNotFound() throws Exception {
        Response response = new Response(1234, remoteAddress);

        responseHandler.channelRead0(null, response);
    }
}
