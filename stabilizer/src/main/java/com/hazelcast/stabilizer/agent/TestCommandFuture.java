package com.hazelcast.stabilizer.agent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestCommandFuture<E> implements Future<E> {

    private final static Object NO_RESULT = new Object(){
        public String toString(){
            return "NO_RESULT";
        }
    };

    private volatile Object result = NO_RESULT;

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
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }
}
