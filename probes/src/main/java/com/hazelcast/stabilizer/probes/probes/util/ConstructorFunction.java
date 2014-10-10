package com.hazelcast.stabilizer.probes.probes.util;

public interface ConstructorFunction<K, V> {
    V createNew(K arg);
}
