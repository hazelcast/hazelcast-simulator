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
package com.hazelcast.simulator.utils;

import com.hazelcast.logging.ILogger;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.FINER;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;
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

    private ILogger iLoggerMock = mock(ILogger.class);
    private ThrottlingLogger throttlingLogger;

    @Test(expected = IllegalArgumentException.class)
    public void testLoggerCannotBeNull() {
        ThrottlingLogger.newLogger(null, 1000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRateCannotBeZero() {
        ThrottlingLogger.newLogger(iLoggerMock, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRateCannotBeNegative() {
        ThrottlingLogger.newLogger(iLoggerMock, -1);
    }

    @Test
    public void testThrottling() {
        long testDurationNanos = SECONDS.toNanos(2);
        long rateMs = 100;
        int threadCount = 2;

        when(iLoggerMock.isLoggable(SEVERE)).thenReturn(true);
        throttlingLogger = ThrottlingLogger.newLogger(iLoggerMock, rateMs);

        startLoggingThreadsAndAwait(threadCount, testDurationNanos);
        assertRightNumberOfInvocation(iLoggerMock, testDurationNanos, rateMs);
    }

    @Test
    public void testIgnoredLevelsAreNotCounted() {
        when(iLoggerMock.isLoggable(WARNING)).thenReturn(true);
        when(iLoggerMock.isLoggable(INFO)).thenReturn(false);
        throttlingLogger = ThrottlingLogger.newLogger(iLoggerMock, 1000);

        for (int i = 0; i < 1000; i++) {
            throttlingLogger.info(MESSAGE);
        }
        verify(iLoggerMock, never()).log(INFO, MESSAGE);

        throttlingLogger.warn(MESSAGE);
        verify(iLoggerMock, times(1)).log(WARNING, MESSAGE);
    }

    @Test
    public void testFine() {
        when(iLoggerMock.isLoggable(FINE)).thenReturn(true);
        throttlingLogger = ThrottlingLogger.newLogger(iLoggerMock, 1000);

        throttlingLogger.fine(MESSAGE);
        verify(iLoggerMock, times(1)).log(FINE, MESSAGE);
    }

    @Test
    public void testFiner() {
        when(iLoggerMock.isLoggable(FINER)).thenReturn(true);
        throttlingLogger = ThrottlingLogger.newLogger(iLoggerMock, 1000);

        throttlingLogger.finer(MESSAGE);
        verify(iLoggerMock, times(1)).log(FINER, MESSAGE);
    }

    @Test
    public void testFinest() {
        when(iLoggerMock.isLoggable(FINEST)).thenReturn(true);
        throttlingLogger = ThrottlingLogger.newLogger(iLoggerMock, 1000);

        throttlingLogger.finest(MESSAGE);
        verify(iLoggerMock, times(1)).log(FINEST, MESSAGE);
    }

    private void assertRightNumberOfInvocation(ILogger logger, long testDurationNanos, long rateMs) {
        int mostInvocation = (int) ((NANOSECONDS.toMillis(testDurationNanos) / rateMs) + 1);

        verify(logger, atLeast(1)).log(SEVERE, MESSAGE);
        verify(logger, atMost(mostInvocation)).log(SEVERE, MESSAGE);
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
