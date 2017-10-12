/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetLogLevel;
import static com.hazelcast.simulator.TestEnvironmentUtils.setLogLevel;
import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.ALL_AGENTS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AbstractServerConnectorTest {

    private static final int DEFAULT_TIMEOUT = 30000;

    private static final int PORT = 10000 + new Random().nextInt(1000);
    private static final int THREAD_POOL_SIZE = 3;
    private static final IntegrationTestOperation DEFAULT_OPERATION = new IntegrationTestOperation();

    private boolean shutdownAfterTest = true;

    private SimulatorAddress connectorAddress;
    private ConcurrentMap<String, ResponseFuture> futureMap;
    private ScheduledExecutorService executorService;
    private ChannelGroup channelGroup;

    private TestServerConnector testServerConnector;

    @Before
    public void before() {
        setLogLevel(Level.TRACE);

        connectorAddress = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
        executorService = mock(ScheduledExecutorService.class);
        channelGroup = mock(ChannelGroup.class);

        testServerConnector = new TestServerConnector(connectorAddress, PORT, THREAD_POOL_SIZE, executorService,
                channelGroup);
        this.futureMap = testServerConnector.futureMap;
    }

    @After
    public void after() {
        resetLogLevel();

        if (shutdownAfterTest) {
            testServerConnector.close();
        }
    }

    @Test(timeout = DEFAULT_TIMEOUT, expected = SimulatorProtocolException.class)
    public void testStart_twice() {
        testServerConnector.start();
        testServerConnector.start();
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testShutdown() throws Exception {
        shutdownAfterTest = false;
        testServerConnector.start();

        testServerConnector.close();

        verify(executorService).shutdown();
        verify(executorService).awaitTermination(anyLong(), any(TimeUnit.class));
    }

    @Test(timeout = DEFAULT_TIMEOUT, expected = SimulatorProtocolException.class)
    public void testShutdown_twice() {
        shutdownAfterTest = false;
        testServerConnector.start();

        testServerConnector.close();
        testServerConnector.close();
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testShutdown_withMessageOnQueue() throws Exception {
        shutdownAfterTest = false;
        testServerConnector.start();

        testServerConnector.submit(COORDINATOR, DEFAULT_OPERATION);
        ResponseFuture future = testServerConnector.submit(COORDINATOR, DEFAULT_OPERATION);

        Thread responseSetter = new Thread() {
            @Override
            public void run() {
                sleepMillis(300);
                setResponse(SUCCESS, 2);
            }
        };

        responseSetter.start();
        testServerConnector.close();
        responseSetter.join();

        Response response = future.get();
        assertEquals(SUCCESS, response.getFirstErrorResponseType());
    }

    @Test(timeout = DEFAULT_TIMEOUT, expected = SimulatorProtocolException.class)
    public void testWrite_withInterruptedException() {
        testServerConnector.start();

        final Thread thread = Thread.currentThread();
        new Thread() {
            @Override
            public void run() {
                sleepMillis(100);

                thread.interrupt();
            }
        }.start();

        testServerConnector.invoke(COORDINATOR, new IntegrationTestOperation());
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testGetEventLoopGroup() {
        testServerConnector.start();

        assertNotNull(testServerConnector.getEventLoopGroup());
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testGetExecutorService() {
        testServerConnector.start();

        assertEquals(executorService, testServerConnector.getScheduledExecutor());
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testGetMessageQueueSizeInternal() throws Exception {
        testServerConnector.submit(COORDINATOR, DEFAULT_OPERATION);
        testServerConnector.submit(COORDINATOR, DEFAULT_OPERATION);
        ResponseFuture future = testServerConnector.submit(COORDINATOR, DEFAULT_OPERATION);

        assertEquals(3, testServerConnector.getMessageQueueSizeInternal());

        testServerConnector.start();
        setResponse(SUCCESS, 3);

        Response response = future.get();
        assertEquals(SUCCESS, response.getFirstErrorResponseType());
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testSubmit_withFailureResponse() throws Exception {
        testServerConnector.start();

        ResponseFuture future = testServerConnector.submit(COORDINATOR, DEFAULT_OPERATION);
        setResponse(EXCEPTION_DURING_OPERATION_EXECUTION, 1);
        Response response = future.get();

        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, response.getFirstErrorResponseType());
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testSubmit_withSendFailure() throws Exception {
        when(channelGroup.writeAndFlush(any())).thenThrow(new RuntimeException("expected"));

        testServerConnector.start();

        ResponseFuture future = testServerConnector.submit(COORDINATOR, DEFAULT_OPERATION);
        Response response = future.get();

        assertEquals(EXCEPTION_DURING_OPERATION_EXECUTION, response.getFirstErrorResponseType());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubmit_whenDestinationContainsWildcard_thenThrowException() {
        testServerConnector.start();

        testServerConnector.submit(ALL_AGENTS, DEFAULT_OPERATION);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteAsync_withSource_whenDestinationContainsWildcard_thenThrowException() {
        testServerConnector.start();

        testServerConnector.invokeAsync(COORDINATOR, ALL_AGENTS, DEFAULT_OPERATION);
    }

    private void setResponse(ResponseType responseType, int expectedMessageCount) {
        int responseSetCounter = 0;
        int tries = 0;
        do {
            for (Map.Entry<String, ResponseFuture> entry : futureMap.entrySet()) {
                ResponseFuture responseFuture = entry.getValue();
                Response response = new Response(responseFuture.getMessageId(), connectorAddress, COORDINATOR, responseType);
                responseFuture.set(response);
                responseSetCounter++;
            }
            sleepMillis(50);
        } while (responseSetCounter < expectedMessageCount && tries++ < 50);
    }

    private class TestServerConnector extends AbstractServerConnector {

        private final ChannelGroup channelGroup;

        TestServerConnector(SimulatorAddress localAddress, int port,
                            int threadPoolSize, ScheduledExecutorService executorService, ChannelGroup channelGroup) {
            super(localAddress, port, threadPoolSize, executorService);

            this.channelGroup = channelGroup;
        }

        @Override
        void configureServerPipeline(ChannelPipeline pipeline, ServerConnector serverConnector) {
        }

        @Override
        ChannelGroup getChannelGroup() {
            return channelGroup;
        }
    }
}
