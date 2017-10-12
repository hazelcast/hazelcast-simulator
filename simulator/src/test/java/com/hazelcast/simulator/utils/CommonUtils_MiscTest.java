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

import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.await;
import static com.hazelcast.simulator.utils.CommonUtils.awaitTermination;
import static com.hazelcast.simulator.utils.CommonUtils.fixRemoteStackTrace;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.CommonUtils.throwableToString;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CommonUtils_MiscTest {

    private static final int DEFAULT_TEST_TIMEOUT = 5000;

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(CommonUtils.class);
    }

    @Test
    public void testGetSimulatorVersion() {
        assertEquals("SNAPSHOT", getSimulatorVersion());
    }

    @Test
    public void testFixRemoteStackTrace() {
        Throwable remoteCause = new Throwable("Expected throwable");
        StackTraceElement[] localSideStackTrace = Thread.currentThread().getStackTrace();
        int expectedLength = remoteCause.getStackTrace().length + localSideStackTrace.length;

        fixRemoteStackTrace(remoteCause, localSideStackTrace);

        assertEquals("Expected stack trace of length %d, but was %d", expectedLength, remoteCause.getStackTrace().length);
    }

    @Test
    public void testRethrow_RuntimeException() {
        Throwable throwable = new RuntimeException();

        try {
            throw rethrow(throwable);
        } catch (RuntimeException e) {
            assertEquals(throwable, e);
        }
    }

    @Test
    public void testRethrow_Throwable() {
        Throwable throwable = new Throwable();

        try {
            throw rethrow(throwable);
        } catch (RuntimeException e) {
            assertEquals(throwable, e.getCause());
        }
    }

    @Test
    public void testThrowableToString() {
        String marker = "#*+*#";
        Throwable throwable = new Throwable(marker);
        String actual = throwableToString(throwable);

        assertTrue(format("Expected throwable string to contain marker %s, but was %s", marker, actual), actual.contains(marker));
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testJoinThread() {
        Thread thread = new Thread() {
            @Override
            public void run() {
                sleepMillis(500);
            }
        };

        thread.start();
        joinThread(thread);
    }

    @Test
    public void testJoinThread_withNull() {
        joinThread(null);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testJoinThread_interrupted() {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean isInterrupted = new AtomicBoolean();

        final Thread thread = new Thread() {
            @Override
            public void run() {
                await(latch);
            }
        };
        Thread joiner = new Thread() {
            @Override
            public void run() {
                joinThread(thread);
                isInterrupted.set(Thread.currentThread().isInterrupted());
            }
        };

        thread.start();
        joiner.start();

        joiner.interrupt();
        joinThread(joiner);

        latch.countDown();
        joinThread(thread);

        assertTrue(isInterrupted.get());
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testJoinThread_withTimeout() {
        Thread thread = new Thread() {
            @Override
            public void run() {
                sleepMillis(500);
            }
        };

        thread.start();
        joinThread(thread, HOURS.toMillis(1));
    }

    @Test
    public void testJoinThread_withTimeout_withNull() {
        joinThread(null, HOURS.toMillis(1));
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testJoinThread_withTimeout_interrupted() {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean isInterrupted = new AtomicBoolean();

        final Thread thread = new Thread() {
            @Override
            public void run() {
                await(latch);
            }
        };
        Thread joiner = new Thread() {
            @Override
            public void run() {
                joinThread(thread, HOURS.toMillis(1));
                isInterrupted.set(Thread.currentThread().isInterrupted());
            }
        };

        thread.start();
        joiner.start();

        joiner.interrupt();
        joinThread(joiner);

        latch.countDown();
        joinThread(thread);

        assertTrue(isInterrupted.get());
    }

    @Test
    public void testJoinThread_whenThreadIsNull_thenNothingHappens() {
        joinThread(null);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testAwait() {
        final CountDownLatch latch = new CountDownLatch(1);

        Thread thread = new Thread() {
            @Override
            public void run() {
                sleepMillis(100);
                latch.countDown();
            }
        };

        thread.start();
        await(latch);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testAwait_whenInterrupted_thenRestoreInterruptedFlag() {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean isInterrupted = new AtomicBoolean();

        Thread waiter = new Thread() {
            @Override
            public void run() {
                await(latch);
                isInterrupted.set(Thread.currentThread().isInterrupted());
            }
        };
        waiter.start();

        waiter.interrupt();
        joinThread(waiter);

        assertEquals(1, latch.getCount());
        assertTrue(isInterrupted.get());
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testAwait_withTimeUnit() {
        final CountDownLatch latch = new CountDownLatch(1);

        Thread thread = new Thread() {
            @Override
            public void run() {
                sleepMillis(100);
                latch.countDown();
            }
        };

        thread.start();
        boolean success = await(latch, 10, SECONDS);

        assertTrue(success);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testAwait_withTimeUnit_whenInterrupted_thenRestoreInterruptedFlag() {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean isInterrupted = new AtomicBoolean();
        final AtomicBoolean isSuccess = new AtomicBoolean();

        Thread waiter = new Thread() {
            @Override
            public void run() {
                boolean success = await(latch, 10, SECONDS);
                isInterrupted.set(Thread.currentThread().isInterrupted());
                isSuccess.set(success);
            }
        };
        waiter.start();

        waiter.interrupt();
        joinThread(waiter);

        assertEquals(1, latch.getCount());
        assertTrue(isInterrupted.get());
        assertFalse(isSuccess.get());
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testAwaitTermination() {
        ExecutorService executorService = ExecutorFactory.createFixedThreadPool(1, "CommonUtilsTest");
        executorService.shutdown();

        awaitTermination(executorService, 5, SECONDS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testAwaitTermination_whenInterrupted_thenRestoreInterruptedFlag() {
        final ExecutorService executorService = ExecutorFactory.createFixedThreadPool(1, "CommonUtilsTest");
        final AtomicBoolean isInterrupted = new AtomicBoolean();

        executorService.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                TimeUnit.DAYS.sleep(1);
                return true;
            }
        });
        executorService.shutdown();

        Thread waiter = new Thread() {
            @Override
            public void run() {
                awaitTermination(executorService, 5, SECONDS);
                isInterrupted.set(Thread.currentThread().isInterrupted());
            }
        };
        waiter.start();

        waiter.interrupt();
        joinThread(waiter);

        assertTrue(isInterrupted.get());
    }
}
