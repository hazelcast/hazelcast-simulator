package com.hazelcast.stabilizer.tests.map.helpers;

import com.hazelcast.core.MapStore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MapStoreWithCounter implements MapStore<Object, Object> {
    private Random random = new Random();

    public static int minDelayMs = 0;
    public static int maxDelayMs = 0;

    public final Map store = new ConcurrentHashMap();
    public AtomicInteger storeCount = new AtomicInteger(0);
    public AtomicInteger deleteCount = new AtomicInteger(0);
    public AtomicInteger countLoad = new AtomicInteger(0);

    public MapStoreWithCounter() {
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
        Map result = new HashMap();
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
            try {
                int delay = minDelayMs + random.nextInt(maxDelayMs);
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString() {
        return "MapStoreWithCounter{" +
                "minDelayMs=" + minDelayMs +
                ", maxDelayMs=" + maxDelayMs +
                ", storeCount=" + storeCount +
                ", deleteCount=" + deleteCount +
                ", countLoad=" + countLoad +
                '}';
    }
}