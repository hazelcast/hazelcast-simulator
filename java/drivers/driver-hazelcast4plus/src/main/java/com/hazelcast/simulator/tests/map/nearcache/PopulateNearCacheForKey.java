/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.map.nearcache;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Populates member-side nearcache using {@link IMap#getAll(Set)}.
 * As sanity check returns information if all keys were found.
 */
class PopulateNearCacheForKey implements Callable<Boolean>, HazelcastInstanceAware, IdentifiedDataSerializable {
    private String mapName;
    private long[] keys;
    private boolean iterative;
    private transient HazelcastInstance hazelcastInstance;

    PopulateNearCacheForKey() {
    }

    public PopulateNearCacheForKey(String mapName, long key) {
        this(mapName, new long[] { key });
    }

    public PopulateNearCacheForKey(String mapName, long[] keys) {
        this.mapName = mapName;
        this.keys = keys;
    }

    @Override
    public Boolean call() throws Exception {
        // return value as sanity check
        IMap<Long, byte[]> map = hazelcastInstance.getMap(mapName);
        if (keys.length > 1) {
            if (iterative) {
                // this path is used to evaluate performance of getAll vs get.
                var values = new ArrayList<byte[]>(keys.length);
                for (var key : keys) {
                    // note: this is a naive, sync implementation. With Pipelining it could be even better
                    var value = map.get(key);
                    if (value != null) {
                        values.add(value);
                    }
                }
                // use value to ensure there are no shortcuts during execution
                return values.size() == keys.length;
            } else {
                var entries = new HashSet<Long>(keys.length);
                for (var key : keys) {
                    entries.add(key);
                }
                // use value to ensure there are no shortcuts during execution
                return map.getAll(entries).size() == keys.length;
            }
        } else {
            return map.get(keys[0]).length > 0;
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    /**
     * Uses individual calls to {@link IMap#get(Object)} instead of {@link IMap#getAll(Set)}
     */
    public PopulateNearCacheForKey iterative(boolean iterative) {
        this.iterative = iterative;
        return this;
    }

    @Override
    public int getFactoryId() {
        return IdentifiedDataSerializableFactory.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return IdentifiedDataSerializableFactory.POPULATE_NEARCACHE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
        out.writeLongArray(keys);
        out.writeBoolean(iterative);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readString();
        keys = in.readLongArray();
        iterative = in.readBoolean();
    }
}
