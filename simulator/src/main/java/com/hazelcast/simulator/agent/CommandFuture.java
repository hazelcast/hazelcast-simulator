package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.worker.commands.Command;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;

public final class CommandFuture<E> implements Future<E> {

    private static final Object NO_RESULT = new Object();

    private final Command command;

    @SuppressWarnings("unchecked")
    private volatile E result = (E) NO_RESULT;

    public CommandFuture(Command command) {
        this.command = command;
    }

    public Command getCommand() {
        return command;
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
            while (result == NO_RESULT) {
                wait();
            }

            if (result instanceof Throwable) {
                throw new ExecutionException((Throwable) result);
            }
            return result;
        }
    }

    @Override
    public E get(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        if (timeout < 0 || timeUnit == null) {
            throw new IllegalArgumentException("Invalid timeout or timeUnit for CommandFuture.get()");
        }

        long remainingTimeoutNanos = timeUnit.toNanos(timeout);
        synchronized (this) {
            while (result == NO_RESULT && remainingTimeoutNanos > 0) {
                long started = System.nanoTime();
                wait(TimeUnit.NANOSECONDS.toMillis(remainingTimeoutNanos));
                remainingTimeoutNanos -= System.nanoTime() - started;
            }

            if (result == NO_RESULT) {
                throw new TimeoutException(format(
                        "Timeout while executing: %s total timeout: %d ms", command, timeUnit.toMillis(timeout)));
            }

            if (result instanceof Throwable) {
                throw new ExecutionException((Throwable) result);
            }
            return result;
        }
    }
}
