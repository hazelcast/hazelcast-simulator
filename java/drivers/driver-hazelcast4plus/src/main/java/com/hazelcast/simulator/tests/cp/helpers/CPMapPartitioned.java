/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.cp.helpers;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CPMapPartitioned<K, V> {

    private final List<CPMap<K, V>> partitions;

    public CPMapPartitioned(HazelcastInstance instance, String name, int partitionCount) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be > 0, was " + partitionCount);
        }

        List<CPMap<K, V>> newPartitions = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            String cpGroupName = name + "-" + i;
            String mapName = name + "@" + cpGroupName;
            newPartitions.add(instance.getCPSubsystem().getMap(mapName));
        }
        this.partitions = Collections.unmodifiableList(newPartitions);
    }

    private CPMap<K, V> getPartition(K key) {
        int index = (key.hashCode() & Integer.MAX_VALUE) % partitions.size();
        return partitions.get(index);
    }

    public V put(K key, V value) {
        return getPartition(key).put(key, value);
    }

    public V putIfAbsent(K key, V value) {
        return getPartition(key).putIfAbsent(key, value);
    }

    public void set(K key, V value) {
        getPartition(key).set(key, value);
    }

    public V remove(K key) {
        return getPartition(key).remove(key);
    }

    public void delete(K key) {
        getPartition(key).delete(key);
    }

    public boolean compareAndSet(K key, V expectedValue, V newValue) {
        return getPartition(key).compareAndSet(key, expectedValue, newValue);
    }

    public V get(K key) {
        return getPartition(key).get(key);
    }
}
