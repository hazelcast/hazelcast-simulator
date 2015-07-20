package com.hazelcast.simulator.protocol.core;

import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MessageFutureTest {

    private static final Response DEFAULT_RESULT = new Response(1L, SimulatorAddress.COORDINATOR, ResponseType.SUCCESS);
    private static final int DEFAULT_TIMEOUT_MS = 500;

    private final MessageFuture<Response> future = MessageFuture.createInstance(
            new ConcurrentHashMap<String, MessageFuture<Response>>(), "future");
    private final FutureSetter futureSetter = new FutureSetter(DEFAULT_RESULT, DEFAULT_TIMEOUT_MS);

    private final MessageFuture<Object> exceptionFuture = MessageFuture.createInstance(
            new ConcurrentHashMap<String, MessageFuture<Object>>(), "exception");
    private final FutureExceptionSetter futureExceptionSetter = new FutureExceptionSetter(exceptionFuture, DEFAULT_TIMEOUT_MS);

    @Test(expected = UnsupportedOperationException.class)
    public void testCancel() throws Exception {
        future.cancel(true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIsCancelled() throws Exception {
        future.isCancelled();
    }

    @Test(timeout = 10000)
    public void testIsDone() throws Exception {
        assertFalse(future.isDone());

        futureSetter.start();
        future.get();

        assertTrue(future.isDone());
    }

    @Test(timeout = 10000)
    public void testGet() throws Exception {
        futureSetter.start();

        Response result = future.get();
        assertEquals(DEFAULT_RESULT, result);
    }

    @Test(timeout = 10000, expected = ExecutionException.class)
    public void testGet_withException() throws Exception {
        futureExceptionSetter.start();

        exceptionFuture.get();
    }

    @Test(timeout = 10000)
    public void testGet_withTimeout() throws Exception {
        futureSetter.start();

        Response result = future.get(10, TimeUnit.SECONDS);
        assertEquals(DEFAULT_RESULT, result);
    }

    @Test(timeout = 10000, expected = TimeoutException.class)
    public void testGet_withTimeout_noResult() throws Exception {
        future.get(50, TimeUnit.MILLISECONDS);
    }

    @Test(timeout = 10000, expected = TimeoutException.class)
    public void testGet_withTimeout_belowOneMilliSecond_noResult() throws Exception {
        future.get(1, TimeUnit.NANOSECONDS);
    }

    @Test(timeout = 10000, expected = TimeoutException.class)
    public void testGet_withTimeout_resultTooLate() throws Exception {
        futureSetter.start();

        future.get(10, TimeUnit.MILLISECONDS);
    }

    @Test(timeout = 10000, expected = ExecutionException.class)
    public void testGet_withTimeout_withException() throws Exception {
        futureExceptionSetter.start();

        exceptionFuture.get(10, TimeUnit.SECONDS);
    }

    @Test(timeout = 10000, expected = IllegalArgumentException.class)
    public void testGet_withTimeout_illegalTimeout() throws Exception {
        future.get(-1, TimeUnit.SECONDS);
    }

    @Test(timeout = 10000, expected = IllegalArgumentException.class)
    public void testGet_withTimeout_illegalTimeUnit() throws Exception {
        future.get(0, null);
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

    private class FutureExceptionSetter extends Thread {

        private final MessageFuture<Object> future;
        private final int delayMs;

        public FutureExceptionSetter(MessageFuture<Object> future, int delayMs) {
            this.future = future;
            this.delayMs = delayMs;
        }

        @Override
        public void run() {
            sleepMillis(delayMs);

            future.set(new RuntimeException("Expected exception!"));
        }
    }
}
