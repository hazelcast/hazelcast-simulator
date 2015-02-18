package com.hazelcast.stabilizer.tests.map.helpers;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MapStoreWithCounterPerKey extends MapStoreWithCounter {

    public Map<Object, AtomicInteger> storeCount = new ConcurrentHashMap<Object, AtomicInteger>();

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
    public int getMaxDelayMs() {
        return 1000;
    }

    @Override
    public int getMinDelayMs() {
        return 100;
    }

}
