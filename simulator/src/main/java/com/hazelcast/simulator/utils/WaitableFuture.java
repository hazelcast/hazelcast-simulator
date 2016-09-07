/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.utils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class WaitableFuture<E> implements Future<E> {

    private static final Object EMPTY = new Object();

    private volatile Object value;

    public WaitableFuture() {
        this(EMPTY);
    }

    public WaitableFuture(Object value) {
        this.value = value;
    }

    public WaitableFuture complete(E v) {
        synchronized (this) {
            if (value != EMPTY) {
                throw new IllegalStateException();
            }
            this.value = v;
            notifyAll();
        }
        return this;
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
        return false;
    }

    @Override
    public E get() throws InterruptedException, ExecutionException {
        synchronized (this) {
            while (value == EMPTY) {
                wait();
            }

            return (E) value;
        }
    }

    @Override
    public E get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }
}
