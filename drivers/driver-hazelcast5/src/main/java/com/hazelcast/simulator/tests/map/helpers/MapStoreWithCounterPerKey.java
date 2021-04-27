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
package com.hazelcast.simulator.tests.map.helpers;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MapStoreWithCounterPerKey extends MapStoreWithCounter {

    private final Map<Object, AtomicInteger> storeCount = new ConcurrentHashMap<>();

    public Set<Object> keySet() {
        return storeCount.keySet();
    }

    public int valueOf(Object key) {
        return storeCount.get(key).intValue();
    }

    @Override
    public void store(Object key, Object value) {
        super.store(key, value);

        if (storeCount.get(key) == null) {
            storeCount.put(key, new AtomicInteger((0)));
        }
        storeCount.get(key).incrementAndGet();
    }

    @Override
    public void storeAll(Map<Object, Object> map) {
        for (Map.Entry<Object, Object> kvp : map.entrySet()) {
            store(kvp.getKey(), kvp.getValue());
        }
    }

    @Override
    public String toString() {
        return "MapStoreWithCounterPerKey{"
                + "storeCountSize=" + storeCount.size()
                + '}';
    }
}
