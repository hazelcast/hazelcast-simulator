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
package com.hazelcast.simulator.tests.icache.helpers;

import javax.cache.integration.CacheLoader;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;

public class RecordingCacheLoader<K> implements CacheLoader<K, K>, Serializable {

    public int loadDelayMs;
    public int loadAllDelayMs;

    private final ConcurrentHashMap<K, K> loaded = new ConcurrentHashMap<>();
    private final AtomicInteger loadCount = new AtomicInteger(0);

    @Override
    public K load(final K key) {
        checkNotNull(key, "load null key!");
        if (loadDelayMs > 0) {
            sleepMillis(loadDelayMs);
        }

        loaded.put(key, key);
        loadCount.incrementAndGet();
        return key;
    }

    @Override
    public Map<K, K> loadAll(Iterable<? extends K> keys) {
        if (loadAllDelayMs > 0) {
            sleepMillis(loadAllDelayMs);
        }

        Map<K, K> map = new HashMap<>();
        for (K key : keys) {
            load(key);
            map.put(key, key);
        }
        return map;
    }

    public boolean hasLoaded(K key) {
        return loaded.containsKey(key);
    }

    @Override
    public String toString() {
        return "RecordingCacheLoader{"
                + "loaded=" + loaded
                + ", loadCount=" + loadCount
                + '}';
    }
}
