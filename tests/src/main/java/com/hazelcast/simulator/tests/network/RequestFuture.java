package com.hazelcast.simulator.tests.network;

import com.hazelcast.simulator.test.TestException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RequestFuture implements Future {

    private final Lock lock = new ReentrantLock(false);
    private final Condition condition = lock.newCondition();
    private volatile Object result;

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

    @Override
    public Object get() throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (result != null) {
            return result;
        }

        long timeoutNs = unit.toNanos(timeout);
        lock.lock();
        try {
            while (result == null) {
                if (timeoutNs <= 0) {
                    throw new TimeoutException();
                }

                timeoutNs = condition.awaitNanos(timeoutNs);
            }

            return result;
        } finally {
            lock.unlock();
        }
    }

    public void set() {
        lock.lock();
        try {
            if (result != null) {
                throw new TestException("result should be null");
            }
            result = Boolean.TRUE;
            condition.signal();
        } finally {
            lock.unlock();
        }
    }

    public void reset() {
        if (result == null) {
            throw new TestException("result can't be null");
        }
        result = null;
    }
}
