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
package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.worker.Promise;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
    public ResponseType get() throws InterruptedException {
        synchronized (this) {
            while (responseType == null) {
                wait();
            }

            return responseType;
        }
    }

    public String getResponse() throws InterruptedException {
        synchronized (this) {
            while (responseType == null) {
                wait();
            }

            return response;
        }
    }

    @Override
    public ResponseType get(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException();
    }
}
