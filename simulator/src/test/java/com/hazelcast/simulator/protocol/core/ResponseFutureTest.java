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
package com.hazelcast.simulator.protocol.core;

import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.simulator.protocol.core.ResponseFuture.createFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createInstance;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.getMessageIdFromFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.getRemoteAddressIndexFromFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.getSourceFromFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseType.INTERRUPTED;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ResponseFutureTest {

    private static final Response DEFAULT_RESULT = new Response(1L, COORDINATOR, COORDINATOR, SUCCESS);
    private static final int DEFAULT_TIMEOUT_MS = 500;

    private final String futureKey = createFutureKey(COORDINATOR, 1, 1);
    private final ResponseFuture future = createInstance(new ConcurrentHashMap<String, ResponseFuture>(), futureKey);
    private final FutureSetter futureSetter = new FutureSetter(DEFAULT_RESULT, DEFAULT_TIMEOUT_MS);

    @Test
    public void testCreateFutureKey() {
        String futureKey = createFutureKey(COORDINATOR, 42, 23);
        assertTrue(futureKey.contains(COORDINATOR.toString()));
        assertTrue(futureKey.contains("42"));
        assertTrue(futureKey.contains("23"));
    }

    @Test
    public void testGetMessageIdFromFutureKey() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.TEST, 4, 8, 23);
        String futureKey = createFutureKey(expectedAddress, 42, 23);
        long messageId = getMessageIdFromFutureKey(futureKey);
        assertEquals(42, messageId);
    }

    @Test
    public void testGetSourceFromFutureKey() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.WORKER, 4, 8, 0);
        String futureKey = createFutureKey(expectedAddress, 42, 23);
        SimulatorAddress actualAddress = getSourceFromFutureKey(futureKey);
        assertEquals(expectedAddress, actualAddress);
    }

    @Test
    public void testGetRemoteAddressIndexFromFutureKey() {
        int expectedRemoteIndex = 23;
        SimulatorAddress workerAddress = new SimulatorAddress(AddressLevel.WORKER, 4, 8, 0);
        String futureKey = createFutureKey(workerAddress, 42, expectedRemoteIndex);
        int actualRemoteIndex = getRemoteAddressIndexFromFutureKey(futureKey);
        assertEquals(expectedRemoteIndex, actualRemoteIndex);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testCancel() {
        future.cancel(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIsCancelled() {
        future.isCancelled();
    }

    @Test(timeout = 10000)
    public void testIsDone() throws Exception {
        assertFalse(future.isDone());

        futureSetter.start();
        future.get();

        assertTrue(future.isDone());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSet_null() {
        future.set(null);
    }

    @Test(timeout = 10000)
    public void testGet() throws Exception {
        futureSetter.start();

        Response result = future.get();
        assertEquals(DEFAULT_RESULT, result);
    }

    @Test(timeout = 10000)
    public void testGet_withTimeout() throws Exception {
        futureSetter.start();

        Response result = future.get(10, TimeUnit.SECONDS);
        assertEquals(DEFAULT_RESULT, result);
    }

    @Test(timeout = 10000, expected = TimeoutException.class)
    public void testGet_withTimeout_noResponse() throws Exception {
        future.get(50, TimeUnit.MILLISECONDS);
    }

    @Test(timeout = 10000, expected = TimeoutException.class)
    public void testGet_withTimeout_belowOneMilliSecond_noResponse() throws Exception {
        future.get(1, TimeUnit.NANOSECONDS);
    }

    @Test(timeout = 10000, expected = TimeoutException.class)
    public void testGet_withTimeout_responseTooLate() throws Exception {
        futureSetter.start();

        future.get(10, TimeUnit.MILLISECONDS);
    }

    @Test(timeout = 10000, expected = IllegalArgumentException.class)
    public void testGet_withTimeout_illegalTimeout() throws Exception {
        future.get(-1, TimeUnit.SECONDS);
    }

    @Test
    public void testGet_interrupted() throws Exception {
        final AtomicBoolean success = new AtomicBoolean();

        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    future.get();
                    fail("Expected InterruptedException!");
                } catch (InterruptedException ignored) {
                    success.set(true);
                }
            }
        };

        thread.start();
        thread.interrupt();
        thread.join();

        assertTrue(success.get());
    }

    @Test
    public void testGetResponse_interrupted() throws Exception {
        final AtomicReference<Response> responseReference = new AtomicReference<Response>();

        Thread thread = new Thread() {
            @Override
            public void run() {
                Response response = future.getResponse();
                responseReference.set(response);
            }
        };

        thread.start();
        thread.interrupt();
        thread.join();

        Response response = responseReference.get();
        assertNotNull(response);
        assertEquals(INTERRUPTED, response.getFirstErrorResponseType());
    }

    private class FutureSetter extends Thread {

        private final Response result;
        private final int delayMs;

        private FutureSetter(Response result, int delayMs) {
            this.result = result;
            this.delayMs = delayMs;
        }

        @Override
        public void run() {
            sleepMillis(delayMs);

            future.set(result);
        }
    }
}
