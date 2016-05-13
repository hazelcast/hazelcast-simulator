package com.hazelcast.simulator.utils;

import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.awaitTermination;
import static com.hazelcast.simulator.utils.CommonUtils.fixRemoteStackTrace;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.CommonUtils.throwableToString;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
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

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testJoinThread_interrupted() {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean isInterrupted = new AtomicBoolean();

        final Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
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
    public void testAwaitTermination() {
        ExecutorService executorService = ExecutorFactory.createFixedThreadPool(1, "CommonUtilsTest");
        executorService.shutdown();

        awaitTermination(executorService, 5, TimeUnit.SECONDS);
    }

    @Test
    public void testAwaitTermination_whenInterrupted_thenRestoreInterruptedFlag() throws Exception {
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
                awaitTermination(executorService, 5, TimeUnit.SECONDS);
                isInterrupted.set(Thread.currentThread().isInterrupted());
            }
        };
        waiter.start();

        waiter.interrupt();
        joinThread(waiter);

        assertTrue(isInterrupted.get());
    }
}
