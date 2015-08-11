package com.hazelcast.simulator.protocol.core;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;

/**
 * A {@link Future} implementation to wait asynchronously for the response of a {@link SimulatorMessage}.
 *
 * @param <E> Value type that the user is expecting
 */
public final class MessageFuture<E> implements Future<E> {

    private static final long ONE_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);
    private static final Object NO_RESULT = new Object();

    private final ConcurrentMap<String, MessageFuture<E>> futureMap;
    private final String key;

    @SuppressWarnings("unchecked")
    private volatile E result = (E) NO_RESULT;

    private MessageFuture(ConcurrentMap<String, MessageFuture<E>> futureMap, String key) {
        this.futureMap = futureMap;
        this.key = key;
    }

    public static <E> MessageFuture<E> createInstance(ConcurrentMap<String, MessageFuture<E>> futureMap, String key) {
        MessageFuture<E> future = new MessageFuture<E>(futureMap, key);
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
        return (result != NO_RESULT);
    }

    public void set(E result) {
        synchronized (this) {
            this.result = result;
            notifyAll();
        }
    }

    @Override
    public E get() throws InterruptedException, ExecutionException {
        synchronized (this) {
            while (NO_RESULT.equals(result)) {
                wait();
            }

            E tmpResult = result;
            if (tmpResult instanceof Throwable) {
                throw new ExecutionException((Throwable) tmpResult);
            }

            futureMap.remove(key);
            return tmpResult;
        }
    }

    @Override
    public E get(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        if (timeout < 0 || timeUnit == null) {
            throw new IllegalArgumentException("Invalid timeout or timeUnit for MessageFuture.get()");
        }

        long remainingTimeoutNanos = timeUnit.toNanos(timeout);
        synchronized (this) {
            while (NO_RESULT.equals(result) && remainingTimeoutNanos > ONE_MILLISECOND) {
                long started = System.nanoTime();
                wait(TimeUnit.NANOSECONDS.toMillis(remainingTimeoutNanos));
                remainingTimeoutNanos -= System.nanoTime() - started;
            }

            E tmpResult = result;
            if (NO_RESULT.equals(tmpResult)) {
                throw new TimeoutException(format("Timeout while waiting for message (%d ms)", timeUnit.toMillis(timeout)));
            }
            if (tmpResult instanceof Throwable) {
                throw new ExecutionException((Throwable) tmpResult);
            }

            futureMap.remove(key);
            return tmpResult;
        }
    }
}
