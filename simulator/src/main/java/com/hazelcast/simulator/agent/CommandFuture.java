package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.worker.commands.Command;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CommandFuture<E> implements Future<E> {

    private static final Object NO_RESULT = new Object() {
        public String toString() {
            return "NO_RESULT";
        }
    };

    private final Command command;
    private volatile Object result = NO_RESULT;

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
        return result != NO_RESULT;
    }

    public void set(Object result) {
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
            return (E) result;
        }
    }

    @Override
    public E get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long remainingTimeoutMs = unit.toMillis(timeout);

        synchronized (this) {
            for (; ; ) {
                if (result != NO_RESULT) {
                    if (result instanceof Throwable) {
                        throw new ExecutionException((Throwable) result);
                    }
                    return (E) result;
                }

                if (remainingTimeoutMs <= 0) {
                    throw new TimeoutException("Timeout while executing : "
                            + command + " total timeout: " + unit.toMillis(timeout) + " ms");
                }

                long startMs = System.currentTimeMillis();
                wait(remainingTimeoutMs);
                remainingTimeoutMs -= System.currentTimeMillis() - startMs;
            }
        }
    }
}
