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
package com.hazelcast.simulator.tests.map.helpers;

import com.hazelcast.core.MapStore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;

public class MapStoreWithCounter<K, V> implements MapStore<K, V> {

    private static int minDelayMs;
    private static int maxDelayMs;

    private final Random random = new Random();
    private final Map<K, V> store = new ConcurrentHashMap<K, V>();
    private final AtomicInteger storeCount = new AtomicInteger(0);
    private final AtomicInteger deleteCount = new AtomicInteger(0);
    private final AtomicInteger countLoad = new AtomicInteger(0);

    public MapStoreWithCounter() {
    }

    public static void setMinMaxDelayMs(int minDelayMs, int maxDelayMs) {
        MapStoreWithCounter.minDelayMs = minDelayMs;
        MapStoreWithCounter.maxDelayMs = maxDelayMs;
    }

    public Object get(K key) {
        return store.get(key);
    }

    public Set<Map.Entry<K, V>> entrySet() {
        return store.entrySet();
    }

    @Override
    public void store(K key, V value) {
        delay();
        storeCount.incrementAndGet();
        store.put(key, value);
    }

    @Override
    public void storeAll(Map<K, V> map) {
        for (Map.Entry<K, V> kvp : map.entrySet()) {
            store(kvp.getKey(), kvp.getValue());
        }
    }

    @Override
    public void delete(K key) {
        delay();
        deleteCount.incrementAndGet();
        store.remove(key);
    }

    @Override
    public void deleteAll(Collection<K> keys) {
        for (K key : keys) {
            delete(key);
        }
    }

    @Override
    public V load(K key) {
        delay();
        countLoad.incrementAndGet();
        return store.get(key);
    }

    @Override
    public Map<K, V> loadAll(Collection<K> keys) {
        Map<K, V> result = new HashMap<K, V>();
        for (K key : keys) {
            final V v = load(key);
            if (v != null) {
                result.put(key, v);
            }
        }
        return result;
    }

    @Override
    public Set<K> loadAllKeys() {
        delay();
        return store.keySet();
    }

    private void delay() {
        if (maxDelayMs != 0) {
            sleepMillis(minDelayMs + random.nextInt(maxDelayMs));
        }
    }

    @Override
    public String toString() {
        return "MapStoreWithCounter{"
                + "minDelayMs=" + minDelayMs
                + ", maxDelayMs=" + maxDelayMs
                + ", storeCount=" + storeCount
                + ", deleteCount=" + deleteCount
                + ", countLoad=" + countLoad
                + '}';
    }
}
