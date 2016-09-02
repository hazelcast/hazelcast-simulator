package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.worker.Promise;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class StubPromise extends Promise implements Future<ResponseType> {
    private volatile ResponseType result;

    @Override
    public void answer(ResponseType responseType, String payload) {
        this.result = responseType;
    }

    public ResponseType join(){
        return result;
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
        return false;
    }

    @Override
    public ResponseType get() throws InterruptedException, ExecutionException {
        return result;
    }

    @Override
    public ResponseType get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return null;
    }
}
