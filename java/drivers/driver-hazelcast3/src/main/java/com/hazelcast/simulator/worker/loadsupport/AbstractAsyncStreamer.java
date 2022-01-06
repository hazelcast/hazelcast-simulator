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
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static java.util.concurrent.TimeUnit.MINUTES;

abstract class AbstractAsyncStreamer<K, V> implements Streamer<K, V> {

    private static final Logger LOGGER = Logger.getLogger(AbstractAsyncStreamer.class);

    private static final long DEFAULT_TIMEOUT_MINUTES = 2;
    private static final int MAXIMUM_LOGGING_RATE_MILLIS = 5000;

    private final int concurrencyLevel;
    private final Semaphore semaphore;
    private final StreamerExecutionCallback callback;
    private final ThrottlingLogger throttlingLogger;
    private final AtomicReference<Throwable> storedException = new AtomicReference<>();
    private final AtomicBoolean rejectedExecutionExceptionReported = new AtomicBoolean();
    private final AtomicBoolean targetDisconnectExceptionReported = new AtomicBoolean();
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
        if (storedException.get() != null) {
            throw new RuntimeException("Aborting pushEntry; problems are detected. Please check the cause",
                    storedException.get());
        }

        acquirePermit(1);
        try {
            ICompletableFuture<V> future = storeAsync(key, value);
            future.andThen(new ExecutionCallback<V>() {
                @Override
                public void onResponse(V response) {
                    callback.onSuccess(response);
                }

                @Override
                public void onFailure(Throwable t) {
                    callback.onFailure(t);
                }
            });
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
        if (storedException.get() != null) {
            throw rethrow(storedException.get());
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

    private final class StreamerExecutionCallback implements BiConsumer<V, Throwable> {

        @Override
        public void accept(V v, Throwable throwable) {
            if (throwable != null) {
                onFailure(throwable);
            } else {
                onSuccess(v);
            }
        }

        public void onSuccess(V response) {
            releasePermit(1);
            counter.incrementAndGet();
        }

        public void onFailure(Throwable t) {
            storedException.compareAndSet(null, t);

            if (t instanceof RejectedExecutionException) {
                // we only want to report the RejectedExecutionException once. With 1000 inflight operations, you will
                // get 1000 of these reports otherwise.
                if (rejectedExecutionExceptionReported.compareAndSet(false, true)) {
                    Exception cause = new Exception("The Streamer ran into a"
                            + " RejectedExecutionException; see the causes for the real cause. Only 1 entry if this"
                            + " exception is reported to prevent exception noise.", t);
                    ExceptionReporter.report(null, cause);
                }
            } else if (t instanceof ExecutionException && t.getCause() instanceof TargetDisconnectedException) {
                // we only want to report the TargetDisconnectedException once. With 1000 inflight operations, you will
                // get 1000 of these reports otherwise.
                if (targetDisconnectExceptionReported.compareAndSet(false, true)) {
                    Exception cause = new Exception("The Streamer ran into a"
                            + " TargetDisconnectedException; see the causes for the real cause. Only 1 entry if this"
                            + " exception is reported to prevent exception noise.", t);
                    ExceptionReporter.report(null, cause);
                }
            } else {
                ExceptionReporter.report(null, t);
            }

            onSuccess(null);
        }
    }
}
