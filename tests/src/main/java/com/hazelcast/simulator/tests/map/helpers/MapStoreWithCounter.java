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

public class MapStoreWithCounter implements MapStore<Object, Object> {
    private static int minDelayMs = 0;
    private static int maxDelayMs = 0;

    private final Random random = new Random();
    private final Map<Object, Object> store = new ConcurrentHashMap<Object, Object>();
    private final AtomicInteger storeCount = new AtomicInteger(0);
    private final AtomicInteger deleteCount = new AtomicInteger(0);
    private final AtomicInteger countLoad = new AtomicInteger(0);

    public MapStoreWithCounter() {
    }

    public static void setMinMaxDelayMs(int minDelayMs, int maxDelayMs) {
        MapStoreWithCounter.minDelayMs = minDelayMs;
        MapStoreWithCounter.maxDelayMs = maxDelayMs;
    }

    public Object get(Object key) {
        return store.get(key);
    }

    public Set<Map.Entry<Object, Object>> entrySet() {
        return store.entrySet();
    }

    @Override
    public void store(Object key, Object value) {
        delay();
        storeCount.incrementAndGet();
        store.put(key, value);
    }

    @Override
    public void storeAll(Map<Object, Object> map) {
        for (Map.Entry<Object, Object> kvp : map.entrySet()) {
            store(kvp.getKey(), kvp.getValue());
        }
    }

    @Override
    public void delete(Object key) {
        delay();
        deleteCount.incrementAndGet();
        store.remove(key);
    }

    @Override
    public void deleteAll(Collection<Object> keys) {
        for (Object key : keys) {
            delete(key);
        }
    }

    @Override
    public Object load(Object key) {
        delay();
        countLoad.incrementAndGet();
        return store.get(key);
    }

    @Override
    public Map<Object, Object> loadAll(Collection<Object> keys) {
        Map<Object, Object> result = new HashMap<Object, Object>();
        for (Object key : keys) {
            final Object v = load(key);
            if (v != null) {
                result.put(key, v);
            }
        }
        return result;
    }

    @Override
    public Set<Object> loadAllKeys() {
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