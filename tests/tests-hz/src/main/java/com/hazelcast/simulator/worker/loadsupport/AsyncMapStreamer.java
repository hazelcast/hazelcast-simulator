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
package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.map.IMap;

import java.util.concurrent.Semaphore;

/**
 * Asynchronous implementation of {@link Streamer} for {@link IMap}.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class AsyncMapStreamer<K, V> extends AbstractAsyncStreamer<K, V> {

    private final IMap<K, V> map;

    AsyncMapStreamer(int concurrencyLevel, IMap<K, V> map) {
        super(concurrencyLevel);
        this.map = map;
    }

    AsyncMapStreamer(int concurrencyLevel, IMap<K, V> map, Semaphore semaphore) {
        super(concurrencyLevel, semaphore);
        this.map = map;
    }

    @Override
    ICompletableFuture storeAsync(K key, V value) {
        return (ICompletableFuture) map.putAsync(key, value);
    }
}
