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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MapStoreWithCounterPerKey extends MapStoreWithCounter<Long, AtomicLong> {

    private final Map<Long, AtomicLong> storeCount = new ConcurrentHashMap<Long, AtomicLong>();

    public Set<Long> keySet() {
        return storeCount.keySet();
    }

    public int valueOf(Long key) {
        return storeCount.get(key).intValue();
    }

    @Override
    public void store(Long key, AtomicLong value) {
        super.store(key, value);

        if (storeCount.get(key) == null) {
            storeCount.put(key, new AtomicLong((0)));
        }
        storeCount.get(key).incrementAndGet();
    }

    @Override
    public void storeAll(Map<Long, AtomicLong> map) {
        for (Map.Entry<Long, AtomicLong> kvp : map.entrySet()) {
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
