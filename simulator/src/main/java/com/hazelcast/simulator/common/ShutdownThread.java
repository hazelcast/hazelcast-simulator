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
package com.hazelcast.simulator.common;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.await;

public abstract class ShutdownThread extends Thread {

    private static final long DEFAULT_WAIT_FOR_SHUTDOWN_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(5);

    private static final Logger LOGGER = Logger.getLogger(ShutdownThread.class);

    private final AtomicBoolean shutdownStarted;
    private final boolean ensureProcessShutdown;
    private final long waitForShutdownTimeoutMillis;
    private final CountDownLatch shutdownComplete;

    protected ShutdownThread(String name, AtomicBoolean shutdownStarted, boolean ensureProcessShutdown) {
        this(name, shutdownStarted, ensureProcessShutdown, DEFAULT_WAIT_FOR_SHUTDOWN_TIMEOUT_MILLIS);
    }

    ShutdownThread(String name, AtomicBoolean shutdownStarted, boolean ensureProcessShutdown,
                   long waitForShutdownTimeoutMillis) {
        this(name, shutdownStarted, ensureProcessShutdown, waitForShutdownTimeoutMillis, new CountDownLatch(1));
    }

    private ShutdownThread(String name, AtomicBoolean shutdownStarted, boolean ensureProcessShutdown,
                           long waitForShutdownTimeoutMillis, CountDownLatch shutdownComplete) {
        super(name);
        setDaemon(false);

        this.shutdownStarted = shutdownStarted;
        this.ensureProcessShutdown = ensureProcessShutdown;
        this.waitForShutdownTimeoutMillis = waitForShutdownTimeoutMillis;
        this.shutdownComplete = shutdownComplete;
    }

    public void awaitShutdown() {
        await(shutdownComplete);
    }

    public boolean awaitShutdownWithTimeout() {
        return await(shutdownComplete, waitForShutdownTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        if (!shutdownStarted.compareAndSet(false, true)) {
            return;
        }

        doRun();

        shutdownComplete.countDown();

        if (ensureProcessShutdown) {
            // ensures that log4j will always flush the log buffers
            LOGGER.info("Stopping log4j...");
            LogManager.shutdown();
        }
    }

    protected abstract void doRun();
}
