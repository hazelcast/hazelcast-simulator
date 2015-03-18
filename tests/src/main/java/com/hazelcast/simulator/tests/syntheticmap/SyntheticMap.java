package com.hazelcast.simulator.tests.syntheticmap;

import com.hazelcast.core.DistributedObject;

public interface SyntheticMap<K,V> extends DistributedObject {

    V get(K key);

    void put(K key, V value);
}
