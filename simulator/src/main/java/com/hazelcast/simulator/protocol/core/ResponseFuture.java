package com.hazelcast.simulator.protocol.core;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;

/**
 * A {@link Future} implementation to wait asynchronously for the {@link Response} to a {@link SimulatorMessage}.
 */
public final class ResponseFuture implements Future<Response> {

    private static final long ONE_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);

    private final ConcurrentMap<String, ResponseFuture> futureMap;
    private final String key;

    private volatile Response response;

    private ResponseFuture(ConcurrentMap<String, ResponseFuture> futureMap, String key) {
        this.futureMap = futureMap;
        this.key = key;
    }

    public static ResponseFuture createInstance(ConcurrentMap<String, ResponseFuture> futureMap, String key) {
        ResponseFuture future = new ResponseFuture(futureMap, key);
        futureMap.put(key, future);

        return future;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDone() {
        return (response != null);
    }

    public void set(Response response) {
        if (response == null) {
            throw new IllegalArgumentException("response is null");
        }

        synchronized (this) {
            this.response = response;
            notifyAll();
        }
    }

    @Override
    public Response get() throws InterruptedException {
        synchronized (this) {
            while (response == null) {
                wait();
            }

            futureMap.remove(key);
            return response;
        }
    }

    @Override
    public Response get(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
        if (timeout < 0 || timeUnit == null) {
            throw new IllegalArgumentException("Invalid timeout or timeUnit for ResponseFuture.get()");
        }

        long remainingTimeoutNanos = timeUnit.toNanos(timeout);
        synchronized (this) {
            while (response == null && remainingTimeoutNanos > ONE_MILLISECOND) {
                long started = System.nanoTime();
                wait(TimeUnit.NANOSECONDS.toMillis(remainingTimeoutNanos));
                remainingTimeoutNanos -= System.nanoTime() - started;
            }

            Response tmpResponse = response;
            if (tmpResponse == null) {
                throw new TimeoutException(format("Timeout while waiting for response (%d ms)", timeUnit.toMillis(timeout)));
            }

            futureMap.remove(key);
            return tmpResponse;
        }
    }
}
