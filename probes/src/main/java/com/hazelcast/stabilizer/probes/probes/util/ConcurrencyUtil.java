package com.hazelcast.stabilizer.probes.probes.util;

import com.hazelcast.stabilizer.probes.probes.util.ConstructorFunction;

import java.util.concurrent.ConcurrentMap;

//this class can be removed once Utils methods are factored out from Stabilizer core
public class ConcurrencyUtil {
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
