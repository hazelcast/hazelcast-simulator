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
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.BuildInfoUtils.isMinVersion;
import static com.hazelcast.simulator.worker.loadsupport.Streamer.DEFAULT_CONCURRENCY_LEVEL;

/**
 * Creates {@link Streamer} instances for {@link IMap} and {@link Cache}.
 *
 * If possible an asynchronous variant is created, otherwise it will be synchronous.
 */
public final class StreamerFactory {

    private static final AtomicBoolean CREATE_ASYNC = new AtomicBoolean(isMinVersion("3.5"));
    private static volatile Boolean USE_REFLECTION_STREAMER = useReflectionAsyncStreamer();

    private StreamerFactory() {
    }

    public static <K, V> Streamer<K, V> getInstance(IMap<K, V> map) {
        return getInstance(map, DEFAULT_CONCURRENCY_LEVEL);
    }

    public static <K, V> Streamer<K, V> getInstance(IMap<K, V> map, int concurrencyLevel) {
        if (CREATE_ASYNC.get()) {
            if (USE_REFLECTION_STREAMER) {
                return new ReflectionAsyncMapStreamer<K, V>(concurrencyLevel, map);
            } else {
                return new AsyncMapStreamer<K, V>(concurrencyLevel, map);
            }
        }
        return new SyncMapStreamer<K, V>(map);
    }

    public static <K, V> Streamer<K, V> getInstance(Cache<K, V> cache) {
        return getInstance(cache, DEFAULT_CONCURRENCY_LEVEL);
    }

    public static <K, V> Streamer<K, V> getInstance(Cache<K, V> cache, int concurrencyLevel) {
        if (CREATE_ASYNC.get() && cache instanceof ICache) {
            return new AsyncCacheStreamer<K, V>(concurrencyLevel, (ICache<K, V>) cache);
        }
        return new SyncCacheStreamer<K, V>(cache);
    }

    static void enforceAsync(boolean enforceAsync) {
        CREATE_ASYNC.set(enforceAsync);
    }

    private static boolean useReflectionAsyncStreamer() {
        IMap imapProxy = null;
        try {
            imapProxy.getAsync(null);
            throw new AssertionError("expected NPE");
        } catch (NoSuchMethodError e) {
            return true;
        } catch (NullPointerException e) {
            return false;
        }
    }
}
