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

import com.hazelcast.cache.ICache;
import com.hazelcast.core.IMap;

import javax.cache.Cache;

import static com.hazelcast.simulator.worker.loadsupport.Streamer.DEFAULT_CONCURRENCY_LEVEL;

/**
 * Creates {@link Streamer} instances for {@link IMap} and {@link Cache}.
 *
 * If possible an asynchronous variant is created, otherwise it will be synchronous.
 */
public final class StreamerFactory {

    private StreamerFactory() {
    }

    public static <K, V> Streamer<K, V> getInstance(IMap<K, V> map) {
        return getInstance(map, DEFAULT_CONCURRENCY_LEVEL);
    }

    public static <K, V> Streamer<K, V> getInstance(IMap<K, V> map, int concurrencyLevel) {
        return new AsyncMapStreamer<>(concurrencyLevel, map);
    }

    public static <K, V> Streamer<K, V> getInstance(Cache<K, V> cache) {
        return getInstance(cache, DEFAULT_CONCURRENCY_LEVEL);
    }

    public static <K, V> Streamer<K, V> getInstance(Cache<K, V> cache, int concurrencyLevel) {
        if (cache instanceof ICache) {
            return new AsyncCacheStreamer<>(concurrencyLevel, (ICache<K, V>) cache);
        }
        return new SyncCacheStreamer<>(cache);
    }
}
