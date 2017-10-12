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
package com.hazelcast.simulator.protocol.core;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.protocol.core.ResponseType.INTERRUPTED;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNBLOCKED_BY_FAILURE;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A {@link Future} implementation to wait asynchronously for the {@link Response} to a {@link SimulatorMessage}.
 */
public final class ResponseFuture implements Future<Response> {

    private static final long ONE_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);

    private final ConcurrentMap<String, ResponseFuture> futureMap;
    private final String key;

    private volatile Response response;
    private ResponseFutureListener listener;

    /**
     * Creates a {@link ResponseFuture} instance.
     *
     * @param futureMap the map of {@link ResponseFuture} where we add this one to
     * @param key       the key for this {@link ResponseFuture} in the map
     */
    public ResponseFuture(ConcurrentMap<String, ResponseFuture> futureMap, String key) {
        this.futureMap = futureMap;
        this.key = key;
    }

    public static ResponseFuture createInstance(ConcurrentMap<String, ResponseFuture> futureMap, String key) {
        ResponseFuture future = new ResponseFuture(futureMap, key);
        if (getMessageIdFromFutureKey(key) > 0) {
            futureMap.put(key, future);
        }
        return future;
    }

    /**
     * Creates a map key for a {@link ResponseFuture}.
     *
     * @param source             the source {@link SimulatorAddress} of a {@link SimulatorMessage}
     * @param messageId          the messageId of a {@link SimulatorMessage}
     * @param remoteAddressIndex the address index of a remote Simulator component
     * @return the key for the {@link ResponseFuture} map
     */
    public static String createFutureKey(SimulatorAddress source, long messageId, int remoteAddressIndex) {
        return source.toString() + '-' + messageId + '-' + remoteAddressIndex;
    }

    static long getMessageIdFromFutureKey(String futureKey) {
        String messageIdString = futureKey.substring(futureKey.indexOf('-') + 1, futureKey.lastIndexOf('-'));
        return parseLong(messageIdString);
    }

    static SimulatorAddress getSourceFromFutureKey(String futureKey) {
        String sourceString = futureKey.substring(0, futureKey.indexOf('-'));
        return SimulatorAddress.fromString(sourceString);
    }

    static int getRemoteAddressIndexFromFutureKey(String futureKey) {
        String remoteAddressIndexString = futureKey.substring(futureKey.lastIndexOf('-') + 1, futureKey.length());
        return parseInt(remoteAddressIndexString);
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
        return (response != null);
    }

    public void unblockOnFailure(SimulatorAddress source, SimulatorAddress destination, int remoteAddressIndex) {
        if (getSourceFromFutureKey(key).equals(destination) && getRemoteAddressIndexFromFutureKey(key) == remoteAddressIndex) {
            long messageId = getMessageIdFromFutureKey(key);
            set(new Response(messageId, destination, source, UNBLOCKED_BY_FAILURE));
        }
    }

    public long getMessageId() {
        return getMessageIdFromFutureKey(key);
    }

    public void set(Response response) {
        if (response == null) {
            throw new IllegalArgumentException("response is null");
        }

        futureMap.remove(key);

        synchronized (this) {
            this.response = response;
            notifyAll();

            if (listener != null) {
                listener.onCompletion(response);
            }
        }
    }

    public Response getResponse() {
        try {
            return get();
        } catch (InterruptedException e) {
            long messageId = getMessageIdFromFutureKey(key);
            SimulatorAddress source = getSourceFromFutureKey(key);
            return new Response(messageId, null, source, INTERRUPTED);
        }
    }

    @Override
    public Response get() throws InterruptedException {
        synchronized (this) {
            while (response == null) {
                wait();
            }

            return response;
        }
    }

    @Override
    public Response get(long timeout, TimeUnit timeUnit) throws InterruptedException, TimeoutException {
        if (timeout < 0) {
            throw new IllegalArgumentException("Invalid timeout or timeUnit for ResponseFuture.get()");
        }

        long remainingTimeoutNanos = timeUnit.toNanos(timeout);
        synchronized (this) {
            while (response == null && remainingTimeoutNanos > ONE_MILLISECOND) {
                long started = System.nanoTime();
                wait(NANOSECONDS.toMillis(remainingTimeoutNanos));
                remainingTimeoutNanos -= System.nanoTime() - started;
            }

            Response tmpResponse = response;
            if (tmpResponse == null) {
                throw new TimeoutException(format("Timeout while waiting for response (%d ms)", timeUnit.toMillis(timeout)));
            }

            return tmpResponse;
        }
    }

    public void addListener(ResponseFutureListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("listener is null");
        }

        synchronized (this) {
            if (this.listener != null) {
                throw new IllegalStateException("Only 1 listener allowed");
            }

            if (response != null) {
                listener.onCompletion(response);
                return;
            }

            this.listener = listener;
        }
    }
}
