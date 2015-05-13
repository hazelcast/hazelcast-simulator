package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;

import java.util.concurrent.Semaphore;

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * Asynchronous implementation of MapStreamer.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class AsyncMapStreamer<K, V> implements MapStreamer<K,V> {

    private static final int DEFAULT_CONCURRENCY_LEVEL = 1000;
    private static final long DEFAULT_TIMEOUT_MINUTES = 2;

    private final IMap<K, V> map;
    private final Semaphore semaphore;
    private final int concurrencyLevel;
    private final ExecutionCallback<V> callback;

    private Throwable storedException;

    AsyncMapStreamer(IMap<K, V> map) {
        this.map = map;
        this.concurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;
        this.semaphore = new Semaphore(concurrencyLevel);
        this.callback = new MyExecutionCallback();
    }

    /**
     * Push key/value pair into a map. It's a non-blocking operation.
     * You have to call {@link #await()} to make sure the entry has been created successfully.
     *
     * @param key   the key of the map entry
     * @param value the new value of the map entry
     */
    @Override
    public void pushEntry(K key, V value) {
        acquirePermit(1);
        ICompletableFuture<V> future = (ICompletableFuture<V>) map.putAsync(key, value);
        future.andThen(callback);
    }

    /**
     * Wait until all in-flight operations are finished.
     *
     * @throws RuntimeException if at least any pushEntry operation failed
     */
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
            rethrow(storedException);
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
            rethrow(e);
        }
    }

    private final class MyExecutionCallback implements ExecutionCallback<V> {

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
