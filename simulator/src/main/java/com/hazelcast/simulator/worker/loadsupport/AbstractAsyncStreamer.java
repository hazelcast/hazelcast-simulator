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
package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.utils.ThrottlingLogger;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static java.util.concurrent.TimeUnit.MINUTES;

abstract class AbstractAsyncStreamer<K, V> implements Streamer<K, V> {

    private static final ILogger LOGGER = Logger.getLogger(AbstractAsyncStreamer.class);

    private static final long DEFAULT_TIMEOUT_MINUTES = 2;
    private static final int MAXIMUM_LOGGING_RATE_MILLIS = 5000;

    private final int concurrencyLevel;
    private final Semaphore semaphore;
    private final ExecutionCallback callback;
    private final ThrottlingLogger throttlingLogger;

    private volatile Throwable storedException;

    private final AtomicLong counter = new AtomicLong();

    AbstractAsyncStreamer(int concurrencyLevel) {
        this(concurrencyLevel, new Semaphore(concurrencyLevel));
    }

    AbstractAsyncStreamer(int concurrencyLevel, Semaphore semaphore) {
        this.concurrencyLevel = concurrencyLevel;
        this.semaphore = semaphore;
        this.callback = new StreamerExecutionCallback();
        this.throttlingLogger = ThrottlingLogger.newLogger(LOGGER, MAXIMUM_LOGGING_RATE_MILLIS);
    }

    abstract ICompletableFuture storeAsync(K key, V value);

    @Override
    @SuppressWarnings("unchecked")
    public void pushEntry(K key, V value) {
        acquirePermit(1);
        try {
            ICompletableFuture<V> future = storeAsync(key, value);
            future.andThen(callback);
        } catch (Exception e) {
            releasePermit(1);

            throw rethrow(e);
        }
    }

    @Override
    public void await() {
        waitForInFlightOperationsFinished();
        releasePermit(concurrencyLevel);
        rethrowExceptionIfAny();
    }

    private void waitForInFlightOperationsFinished() {
        acquirePermit(concurrencyLevel);
    }

    private void rethrowExceptionIfAny() {
        if (storedException != null) {
            throw rethrow(storedException);
        }
    }

    private void releasePermit(int count) {
        semaphore.release(count);
        throttlingLogger.info("At: " + counter.get());
    }

    private void acquirePermit(int count) {
        throttlingLogger.info("At: " + counter.get());
        try {
            if (!semaphore.tryAcquire(count, DEFAULT_TIMEOUT_MINUTES, MINUTES)) {
                throw new IllegalStateException("Timeout when trying to acquire a permit! Completed: " + counter.get());
            }
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    private final class StreamerExecutionCallback implements ExecutionCallback<V> {

        @Override
        public void onResponse(V response) {
            releasePermit(1);
            counter.incrementAndGet();
        }

        @Override
        public void onFailure(Throwable t) {
            ExceptionReporter.report(null, t);
            storedException = t;

            releasePermit(1);
            counter.incrementAndGet();
        }
    }
}
