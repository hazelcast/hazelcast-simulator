package com.hazelcast.stabilizer.tests.icache.helpers;

import javax.cache.integration.CacheLoader;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.stabilizer.utils.CommonUtils.sleepMillis;

public class RecordingCacheLoader<K> implements CacheLoader<K, K>, Serializable {

    public ConcurrentHashMap<K, K> loaded = new ConcurrentHashMap<K, K>();
    public AtomicInteger loadCount = new AtomicInteger(0);

    public int loadDelayMs=0;
    public int loadAllDelayMs=0;

    @Override
    public K load(final K key) {

        if (loadDelayMs > 0) {
            sleepMillis(loadDelayMs);
        }

        if (key == null) {
            throw new NullPointerException("load Null key!");
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

        Map<K, K> map = new HashMap<K, K>();
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
        return "RecordingCacheLoader{" +
                "loaded=" + loaded +
                ", loadCount=" + loadCount +
                '}';
    }
}