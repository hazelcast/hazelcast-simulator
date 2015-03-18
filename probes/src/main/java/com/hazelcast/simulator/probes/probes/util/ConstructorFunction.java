package com.hazelcast.simulator.probes.probes.util;

// this class can be removed once util methods are in a separate module
public interface ConstructorFunction<K, V> {
    V createNew(K arg);
}
