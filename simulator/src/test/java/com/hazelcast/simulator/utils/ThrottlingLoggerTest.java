package com.hazelcast.simulator.utils;

import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.log4j.Level.DEBUG;
import static org.apache.log4j.Level.FATAL;
import static org.apache.log4j.Level.INFO;
import static org.apache.log4j.Level.TRACE;
import static org.apache.log4j.Level.WARN;
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

    private Logger loggerMock = mock(Logger.class);
    private ThrottlingLogger throttlingLogger;

    @Test(expected = IllegalArgumentException.class)
    public void testLoggerCannotBeNull() {
        ThrottlingLogger.newLogger(null, 1000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRateCannotBeZero() {
        ThrottlingLogger.newLogger(loggerMock, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRateCannotBeNegative() {
        ThrottlingLogger.newLogger(loggerMock, -1);
    }

    @Test
    public void testThrottling() {
        long testDurationNanos = SECONDS.toNanos(2);
        long rateMs = 100;
        int threadCount = 2;

        when(loggerMock.isEnabledFor(FATAL)).thenReturn(true);
        throttlingLogger = ThrottlingLogger.newLogger(loggerMock, rateMs);

        startLoggingThreadsAndAwait(threadCount, testDurationNanos);
        assertRightNumberOfInvocation(loggerMock, testDurationNanos, rateMs);
    }

    @Test
    public void testIgnoredLevelsAreNotCounted() {
        when(loggerMock.isEnabledFor(WARN)).thenReturn(true);
        when(loggerMock.isEnabledFor(INFO)).thenReturn(false);
        throttlingLogger = ThrottlingLogger.newLogger(loggerMock, 1000);

        for (int i = 0; i < 1000; i++) {
            throttlingLogger.info(MESSAGE);
        }
        verify(loggerMock, never()).log(INFO, MESSAGE);

        throttlingLogger.warn(MESSAGE);
        verify(loggerMock, times(1)).log(WARN, MESSAGE);
    }

    @Test
    public void testFine() {
        when(loggerMock.isEnabledFor(DEBUG)).thenReturn(true);
        throttlingLogger = ThrottlingLogger.newLogger(loggerMock, 1000);

        throttlingLogger.fine(MESSAGE);
        verify(loggerMock, times(1)).log(DEBUG, MESSAGE);
    }

    @Test
    public void testTrace() {
        when(loggerMock.isEnabledFor(TRACE)).thenReturn(true);
        throttlingLogger = ThrottlingLogger.newLogger(loggerMock, 1000);

        throttlingLogger.finer(MESSAGE);
        verify(loggerMock, times(1)).log(TRACE, MESSAGE);
    }

    private void assertRightNumberOfInvocation(Logger logger, long testDurationNanos, long rateMs) {
        int mostInvocation = (int) ((NANOSECONDS.toMillis(testDurationNanos) / rateMs) + 1);

        verify(logger, atLeast(1)).log(FATAL, MESSAGE);
        verify(logger, atMost(mostInvocation)).log(FATAL, MESSAGE);
    }

    private void startLoggingThreadsAndAwait(int threadCount, long testDurationNanos) {
        CountDownLatch latch = new CountDownLatch(threadCount);
        long startTime = System.nanoTime();
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
                    throttlingLogger.severe(MESSAGE);
                } while (System.nanoTime() < startTime + testDurationNanos);
                finishLatch.countDown();
            }
        }.start();
    }
}
