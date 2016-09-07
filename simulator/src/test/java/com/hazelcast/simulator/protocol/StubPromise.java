package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.worker.Promise;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StubPromise extends Promise implements Future<ResponseType> {


    private volatile ResponseType responseType;
    private volatile String response;

    @Override
    public void answer(ResponseType responseType, String payload) {
        synchronized (this) {
            if (this.responseType != null) {
                return;
            }

            this.responseType = responseType;
            this.response = payload;
            notifyAll();
        }
    }

    public ResponseType join() {
        try {
            return get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return responseType != null;
    }

    @Override
    public ResponseType get() throws InterruptedException, ExecutionException {
        synchronized (this) {
            while (responseType == null) {
                wait();
            }

            return responseType;
        }
    }

    @Override
    public ResponseType get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }
}
