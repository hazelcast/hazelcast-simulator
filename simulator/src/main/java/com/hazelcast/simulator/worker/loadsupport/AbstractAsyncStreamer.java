/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import java.util.concurrent.Semaphore;

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static java.util.concurrent.TimeUnit.MINUTES;

abstract class AbstractAsyncStreamer<K, V> implements Streamer<K, V> {

    private static final int DEFAULT_CONCURRENCY_LEVEL = 1000;
    private static final long DEFAULT_TIMEOUT_MINUTES = 2;

    private final int concurrencyLevel;
    private final Semaphore semaphore;
    private final ExecutionCallback callback;

    private volatile Throwable storedException;

    AbstractAsyncStreamer() {
        this.concurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;
        this.semaphore = new Semaphore(DEFAULT_CONCURRENCY_LEVEL);
        this.callback = new StreamerExecutionCallback();
    }

    abstract ICompletableFuture storeAsync(K key, V value);

    @Override
    @SuppressWarnings("unchecked")
    public void pushEntry(K key, V value) {
        acquirePermit(1);
        ICompletableFuture<V> future = storeAsync(key, value);
        future.andThen(callback);
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
    }

    private void acquirePermit(int count) {
        try {
            if (!semaphore.tryAcquire(count, DEFAULT_TIMEOUT_MINUTES, MINUTES)) {
                throw new IllegalStateException("Timeout when trying to acquire a permit!");
            }
        } catch (InterruptedException e) {
            throw rethrow(e);
        }
    }

    private final class StreamerExecutionCallback implements ExecutionCallback<V> {

        @Override
        public void onResponse(V response) {
            releasePermit(1);
        }

        @Override
        public void onFailure(Throwable t) {
            storedException = t;
            releasePermit(1);
        }
    }
}
