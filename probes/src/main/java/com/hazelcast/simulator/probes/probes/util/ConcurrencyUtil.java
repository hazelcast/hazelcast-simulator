package com.hazelcast.simulator.probes.probes.util;

import java.util.concurrent.ConcurrentMap;

// this class can be removed once util methods are in a separate module
public final class ConcurrencyUtil {

    private ConcurrencyUtil() {
    }

    public static <K, V> V getOrPutIfAbsent(ConcurrentMap<K, V> map, K key, ConstructorFunction<K, V> func) {
        V value = map.get(key);
        if (value == null) {
            value = func.createNew(key);
            V current = map.putIfAbsent(key, value);
            value = current == null ? value : current;
        }
        return value;
    }

}
