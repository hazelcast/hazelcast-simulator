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

    private Throwable storedException;

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
        ICompletableFuture future = storeAsync(key, value);
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

    private final class StreamerExecutionCallback implements ExecutionCallback {

        @Override
        public void onResponse(Object response) {
            releasePermit(1);
        }

        @Override
        public void onFailure(Throwable t) {
            storedException = t;
            releasePermit(1);
        }
    }
}
