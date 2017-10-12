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
package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.utils.helper.CallerInterrupter;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillisThrowException;
import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;
import static com.hazelcast.simulator.utils.CommonUtils.sleepRandomNanos;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSecondsThrowException;
import static com.hazelcast.simulator.utils.CommonUtils.sleepTimeUnit;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

public class CommonUtils_SleepTest {

    private static final long ONE_SECOND_TO_NANOS = SECONDS.toNanos(1);

    @Test
    public void testSleepSecondsZero() {
        long started = System.nanoTime();
        sleepSeconds(0);
        long duration = getElapsedSeconds(started);

        long durationLimit = SECONDS.toSeconds(3);
        assertTrue(format("Expected sleep duration < %d s, but was %d", durationLimit, duration), duration < durationLimit);
    }

    @Test
    public void testSleepSeconds() {
        long started = System.nanoTime();
        sleepSeconds(1);
        long duration = getElapsedSeconds(started);

        long durationLimit = SECONDS.toSeconds(3);
        assertTrue(format("Expected sleep duration > 0 s, but was %d", duration), duration > 0);
        assertTrue(format("Expected sleep duration < %d s, but was %d", durationLimit, duration), duration < durationLimit);
    }

    @Test
    public void testSleepSecondsInterrupted() {
        Thread sleeperThread = new Thread() {
            @Override
            public void run() {
                sleepSeconds(Integer.MAX_VALUE);
            }
        };
        sleeperThread.start();
        sleeperThread.interrupt();
    }

    @Test
    public void testSleepMillisZero() {
        long started = System.nanoTime();
        sleepMillis(0);
        long duration = NANOSECONDS.toMillis(System.nanoTime() - started);

        long durationLimit = SECONDS.toMillis(3);
        assertTrue(format("Expected sleep duration < %d ms, but was %d", durationLimit, duration), duration < durationLimit);
    }

    @Test
    public void testSleepMillis() {
        long started = System.nanoTime();
        sleepMillis(1);
        long duration = NANOSECONDS.toMillis(System.nanoTime() - started);

        long durationLimit = SECONDS.toMillis(3);
        assertTrue(format("Expected sleep duration > 0 ms, but was %d", duration), duration > 0);
        assertTrue(format("Expected sleep duration < %d ms, but was %d", durationLimit, duration), duration < durationLimit);
    }

    @Test
    public void testSleepMillisInterrupted() {
        Thread sleeperThread = new Thread() {
            @Override
            public void run() {
                sleepMillis(Integer.MAX_VALUE);
            }
        };
        sleeperThread.start();
        sleeperThread.interrupt();
    }

    @Test
    public void testSleepNanosZero() {
        long started = System.nanoTime();
        sleepNanos(0);
        long duration = NANOSECONDS.toNanos(System.nanoTime() - started);

        long durationLimit = SECONDS.toNanos(3);
        assertTrue(format("Expected sleep duration < %d ns, but was %d", durationLimit, duration), duration < durationLimit);
    }

    @Test
    public void testSleepNanos() {
        long started = System.nanoTime();
        sleepNanos(1);
        long duration = NANOSECONDS.toNanos(System.nanoTime() - started);

        long durationLimit = SECONDS.toNanos(3);
        assertTrue(format("Expected sleep duration > 0 ns, but was %d", duration), duration > 0);
        assertTrue(format("Expected sleep duration < %d ns, but was %d", durationLimit, duration), duration < durationLimit);
    }

    @Test
    public void testSleepNanosInterrupted() {
        Thread sleeperThread = new Thread() {
            @Override
            public void run() {
                sleepNanos(Long.MAX_VALUE);
            }
        };
        sleeperThread.start();
        sleeperThread.interrupt();
    }

    @Test
    public void testSleepTimeUnit() {
        long started = System.nanoTime();
        sleepTimeUnit(MILLISECONDS, 1);
        long duration = NANOSECONDS.toMillis(System.nanoTime() - started);

        long durationLimit = SECONDS.toMillis(3);
        assertTrue(format("Expected sleep duration > 0 ms, but was %d", duration), duration > 0);
        assertTrue(format("Expected sleep duration < %d ms, but was %d", durationLimit, duration), duration < durationLimit);
    }

    @Test
    public void testSleepTimeUnitInterrupted() {
        Thread sleeperThread = new Thread() {
            @Override
            public void run() {
                sleepTimeUnit(TimeUnit.DAYS, Long.MAX_VALUE);
            }
        };
        sleeperThread.start();
        sleeperThread.interrupt();
    }

    @Test
    public void testSleepSecondsThrowException() {
        sleepSecondsThrowException(1);
    }

    @Test(expected = RuntimeException.class)
    public void testSleepSecondsThrowExceptionInterrupted() {
        interruptThisThreadAfter(ONE_SECOND_TO_NANOS);

        sleepSecondsThrowException(Integer.MAX_VALUE);
    }

    @Test
    public void testSleepMillisThrowException() {
        sleepMillisThrowException(100);
    }

    @Test(expected = RuntimeException.class)
    public void testSleepMillisThrowExceptionInterrupted() {
        interruptThisThreadAfter(ONE_SECOND_TO_NANOS);

        sleepMillisThrowException(Integer.MAX_VALUE);
    }

    @Test
    public void testSleepRandomNanosMinDelayZero() {
        long started = System.nanoTime();
        sleepRandomNanos(new Random(), 0);
        long duration = System.nanoTime() - started;

        long durationLimit = ONE_SECOND_TO_NANOS;
        assertTrue(format("Expected sleep duration > 0 ns, but was %d", duration), duration > 0);
        assertTrue(format("Expected sleep duration < %d ns, but was %d", durationLimit, duration), duration < durationLimit);
    }

    @Test
    public void testSleepRandomNanos() {
        long started = System.nanoTime();
        sleepRandomNanos(new Random(), 2000);
        long duration = System.nanoTime() - started;

        long durationLimit = ONE_SECOND_TO_NANOS;
        assertTrue(format("Expected sleep duration > 0 ns, but was %d", duration), duration > 0);
        assertTrue(format("Expected sleep duration < %d ns, but was %d", durationLimit, duration), duration < durationLimit);
    }

    private static void interruptThisThreadAfter(long sleepNanos) {
        new CallerInterrupter(Thread.currentThread(), sleepNanos).start();
    }
}
