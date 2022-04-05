package com.hazelcast.simulator.common;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.await;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertFalse;

public class ShutdownThreadTest {

    private CountDownLatch latch = new CountDownLatch(1);

    @Test
    public void testAwaitShutdown() {
        ShutdownThreadImpl thread = new ShutdownThreadImpl();
        thread.start();

        new UnblockThread().start();

        thread.awaitShutdown();
    }

    @Test
    public void testAwaitShutdownWithTimeout() {
        ShutdownThreadImpl thread = new ShutdownThreadImpl();
        thread.start();

        boolean success = thread.awaitShutdownWithTimeout();
        latch.countDown();
        thread.awaitShutdown();

        assertFalse(success);
    }

    private final class UnblockThread extends Thread {
        @Override
        public void run() {
            sleepMillis(100);
            latch.countDown();
        }
    }

    private final class ShutdownThreadImpl extends ShutdownThread {

        private ShutdownThreadImpl() {
            super("ShutdownThreadImpl", new AtomicBoolean(), false, 300);
        }

        @Override
        protected void doRun() {
            await(latch);
        }
    }
}
