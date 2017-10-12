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
package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.protocol.core.ResponseFuture.createFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createInstance;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoordinatorRemoteConnectorTest {

    private static final int DEFAULT_TIMEOUT = 30000;

    private CoordinatorRemoteConnector connector;

    private static final int COORDINATOR_PORT = 0;

    @Before
    public void before() {
        connector = new CoordinatorRemoteConnector("127.0.0.1", COORDINATOR_PORT);
    }

    @After
    public void after() {
        connector.close();
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testWrite_withInterruptedException() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean exceptionThrown = new AtomicBoolean(false);

        String futureKey = createFutureKey(COORDINATOR, 1, 1);
        ResponseFuture responseFuture = createInstance(connector.getFutureMap(), futureKey);

        ClientConnector coordinator = mock(ClientConnector.class);
        when(coordinator.writeAsync(any(SimulatorMessage.class))).thenReturn(responseFuture);

        connector.addCoordinator(coordinator);

        Thread thread = new Thread() {
            @Override
            public void run() {
                latch.countDown();
                try {
                    connector.write(new IntegrationTestOperation());
                } catch (SimulatorProtocolException e) {
                    exceptionThrown.set(true);
                }
            }
        };
        thread.start();

        latch.await();
        thread.interrupt();

        joinThread(thread);

        assertTrue("Expected SimulatorProtocolException to be thrown, but flag was not set", exceptionThrown.get());
    }
}
