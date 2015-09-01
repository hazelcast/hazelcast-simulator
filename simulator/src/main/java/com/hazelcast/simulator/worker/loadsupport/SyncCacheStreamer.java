package com.hazelcast.simulator.worker.loadsupport;

import javax.cache.Cache;

public class SyncCacheStreamer<K, V> implements Streamer<K, V> {

    private final Cache<K, V> cache;

    SyncCacheStreamer(Cache<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public void pushEntry(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public void await() {
    }
}
