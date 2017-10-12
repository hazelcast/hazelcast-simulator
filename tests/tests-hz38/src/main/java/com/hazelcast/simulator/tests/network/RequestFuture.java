/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.network;

import com.hazelcast.simulator.test.TestException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

public class RequestFuture implements Future {

    Thread thread;
    private volatile Object result;

    public RequestFuture() {
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
        while (result == null) {
            if (timeoutNs <= 0) {
                throw new TimeoutException();
            }
            long startNs = System.nanoTime();
            LockSupport.parkNanos(timeout);
            long durationNs = System.nanoTime() - startNs;
            timeoutNs -= durationNs;
        }

        return result;
    }

    public void set() {
        if (result != null) {
            throw new TestException("result should be null");
        }
        result = Boolean.TRUE;
        LockSupport.unpark(thread);
    }

    public void reset() {
        if (result == null) {
            throw new TestException("result can't be null");
        }
        result = null;
    }
}
