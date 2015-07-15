package com.hazelcast.simulator.tests.map.helpers;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MapStoreWithCounterPerKey extends MapStoreWithCounter {

    private final Map<Object, AtomicInteger> storeCount = new ConcurrentHashMap<Object, AtomicInteger>();

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
