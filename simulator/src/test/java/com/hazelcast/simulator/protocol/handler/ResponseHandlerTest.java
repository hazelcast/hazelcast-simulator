/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    public void before() {
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
    public void testChannelRead0_futureNotFound() {
        Response response = new Response(1234, remoteAddress);

        responseHandler.channelRead0(null, response);
    }
}
