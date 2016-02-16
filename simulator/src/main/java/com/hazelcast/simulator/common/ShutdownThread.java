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
package com.hazelcast.simulator.common;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ShutdownThread extends Thread {

    private static final long DEFAULT_WAIT_FOR_SHUTDOWN_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(5);

    private static final Logger LOGGER = Logger.getLogger(ShutdownThread.class);

    private final AtomicBoolean shutdownStarted;
    private final boolean shutdownLog4j;
    private final long waitForShutdownTimeoutMillis;
    private final CountDownLatch shutdownComplete;

    protected ShutdownThread(String name, AtomicBoolean shutdownStarted, boolean shutdownLog4j) {
        this(name, shutdownStarted, shutdownLog4j, DEFAULT_WAIT_FOR_SHUTDOWN_TIMEOUT_MILLIS);
    }

    protected ShutdownThread(String name, AtomicBoolean shutdownStarted, boolean shutdownLog4j, CountDownLatch shutdownComplete) {
        this(name, shutdownStarted, shutdownLog4j, DEFAULT_WAIT_FOR_SHUTDOWN_TIMEOUT_MILLIS, shutdownComplete);
    }

    ShutdownThread(String name, AtomicBoolean shutdownStarted, boolean shutdownLog4j,
                             long waitForShutdownTimeoutMillis) {
        this(name, shutdownStarted, shutdownLog4j, waitForShutdownTimeoutMillis, new CountDownLatch(1));
    }

    private ShutdownThread(String name, AtomicBoolean shutdownStarted, boolean shutdownLog4j,
                             long waitForShutdownTimeoutMillis, CountDownLatch shutdownComplete) {
        super(name);
        setDaemon(false);

        this.shutdownStarted = shutdownStarted;
        this.shutdownLog4j = shutdownLog4j;
        this.waitForShutdownTimeoutMillis = waitForShutdownTimeoutMillis;
        this.shutdownComplete = shutdownComplete;
    }

    public void awaitShutdown() throws Exception {
        shutdownComplete.await();
    }

    public boolean awaitShutdownWithTimeout() throws Exception {
        return shutdownComplete.await(waitForShutdownTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        if (!shutdownStarted.compareAndSet(false, true)) {
            return;
        }

        doRun();

        // ensures that log4j will always flush the log buffers
        if (shutdownLog4j) {
            LOGGER.info("Stopping log4j...");
            LogManager.shutdown();
        }

        shutdownComplete.countDown();
    }

    protected abstract void doRun();
}
