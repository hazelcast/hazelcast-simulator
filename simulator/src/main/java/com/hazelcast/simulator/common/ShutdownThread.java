package com.hazelcast.simulator.common;

import com.hazelcast.simulator.protocol.operation.OperationTypeCounter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ShutdownThread extends Thread {

    private static final Logger LOGGER = Logger.getLogger(ShutdownThread.class);

    private final CountDownLatch shutdownComplete = new CountDownLatch(1);

    private final AtomicBoolean shutdownStarted;
    private final boolean shutdownLog4j;

    protected ShutdownThread(String name, AtomicBoolean shutdownStarted, boolean shutdownLog4j) {
        super(name);
        setDaemon(true);

        this.shutdownStarted = shutdownStarted;
        this.shutdownLog4j = shutdownLog4j;
    }

    public void awaitShutdown() throws Exception {
        shutdownComplete.await();
    }

    @Override
    public void run() {
        if (!shutdownStarted.compareAndSet(false, true)) {
            return;
        }

        doRun();

        OperationTypeCounter.printStatistics();

        LOGGER.info("Stopping log4j...");
        if (shutdownLog4j) {
            // makes sure that log4j will always flush the log buffers
            LogManager.shutdown();
        }

        shutdownComplete.countDown();
    }

    protected abstract void doRun();
}
