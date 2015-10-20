package com.hazelcast.simulator.protocol.core;

import com.hazelcast.util.EmptyStatement;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.protocol.core.ResponseFuture.createFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createInstance;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.getMessageIdFromFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.getSourceFromFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ResponseFutureTest {

    private static final Response DEFAULT_RESULT = new Response(1L, COORDINATOR, COORDINATOR, SUCCESS);
    private static final int DEFAULT_TIMEOUT_MS = 500;

    private final ResponseFuture future = createInstance(new ConcurrentHashMap<String, ResponseFuture>(), "key");
    private final FutureSetter futureSetter = new FutureSetter(DEFAULT_RESULT, DEFAULT_TIMEOUT_MS);

    @Test
    public void testCreateFutureKey() {
        String futureKey = createFutureKey(COORDINATOR, 42, 23);
        assertTrue(futureKey.contains(COORDINATOR.toString()));
        assertTrue(futureKey.contains("42"));
        assertTrue(futureKey.contains("23"));
    }

    @Test
    public void testGetSourceFromFutureKey() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.WORKER, 4, 8, 0);
        String futureKey = createFutureKey(expectedAddress, 42, 23);
        SimulatorAddress actualAddress = getSourceFromFutureKey(futureKey);
        assertEquals(expectedAddress, actualAddress);
    }

    @Test
    public void testGetMessageIdFromFutureKey() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.TEST, 4, 8, 23);
        String futureKey = createFutureKey(expectedAddress, 42, 23);
        long messageId = getMessageIdFromFutureKey(futureKey);
        assertEquals(42, messageId);
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

    @Test(timeout = 10000, expected = IllegalArgumentException.class)
    public void testGet_withTimeout_illegalTimeUnit() throws Exception {
        future.get(0, null);
    }

    @Test
    public void testGet_interrupted() throws Exception {
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    future.get();
                    fail("Expected InterruptedException!");
                } catch (InterruptedException ignored) {
                    EmptyStatement.ignore(ignored);
                }
            }
        };

        thread.start();
        thread.interrupt();
        thread.join();
    }

    private class FutureSetter extends Thread {

        private final Response result;
        private final int delayMs;

        public FutureSetter(Response result, int delayMs) {
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
