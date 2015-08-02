package com.hazelcast.simulator.utils;

import com.hazelcast.logging.ILogger;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ThrottlingLoggerTest {

    private static final String MESSAGE = "message";

    private ThrottlingLogger throttlingLogger;

    @Test(expected = IllegalArgumentException.class)
    public void testLoggerCannotBeNull() {
        ThrottlingLogger.newLogger(null, 1000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRateCannotBeZero() {
        ILogger logger = mock(ILogger.class);
        ThrottlingLogger.newLogger(logger, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRateCannotBeNegative() {
        ILogger logger = mock(ILogger.class);
        ThrottlingLogger.newLogger(logger, -1);
    }

    @Test
    public void testThrottling() {
        final long testDurationNanos = SECONDS.toNanos(2);
        long rateMs = 100;
        int threadCount = 2;

        ILogger logger = mock(ILogger.class);
        when(logger.isLoggable(SEVERE)).thenReturn(true);
        throttlingLogger = ThrottlingLogger.newLogger(logger, rateMs);

        startLoggingThreadsAndAwait(threadCount, testDurationNanos);
        assertRightNumberOfInvocation(logger, testDurationNanos, rateMs);
    }

    @Test
    public void testIgnoredLevelsAreNotCounted() {
        ILogger logger = mock(ILogger.class);
        when(logger.isLoggable(SEVERE)).thenReturn(true);
        when(logger.isLoggable(INFO)).thenReturn(false);
        throttlingLogger = ThrottlingLogger.newLogger(logger, 1000);

        for (int i = 0; i < 1000; i++) {
            throttlingLogger.log(INFO, MESSAGE);
        }
        verify(logger, never()).log(INFO, MESSAGE);

        throttlingLogger.log(SEVERE, MESSAGE);
        verify(logger, times(1)).log(SEVERE, MESSAGE);
    }

    private void assertRightNumberOfInvocation(ILogger logger, long testDurationNanos, long rateMs) {
        int mostInvocation = (int) ((NANOSECONDS.toMillis(testDurationNanos) / rateMs) + 1);
        verify(logger, atMost(mostInvocation)).log(SEVERE, MESSAGE);
        verify(logger, atLeast(1)).log(SEVERE, MESSAGE);
    }

    private void startLoggingThreadsAndAwait(int threadCount, final long testDurationNanos) {
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final long startTime = System.nanoTime();
        for (int i = 0; i < threadCount; i++) {
            startLoggerThread(startTime, testDurationNanos, latch);
        }
        try {
            boolean await = latch.await(testDurationNanos * 10, TimeUnit.NANOSECONDS);
            assertTrue("Timeout when waiting for worker threads", await);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void startLoggerThread(final long startTime, final long testDurationNanos, final CountDownLatch finishLatch) {
        new Thread() {
            @Override
            public void run() {
                do {
                    throttlingLogger.log(SEVERE, MESSAGE);
                } while (System.nanoTime() < startTime + testDurationNanos);
                finishLatch.countDown();
            }
        }.start();
    }
}
