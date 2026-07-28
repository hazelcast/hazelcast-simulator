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
import com.hazelcast.map.impl.proxy.NearCachedMapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Callable;

/**
 * Invalidates near cache associated with member-side proxy. The entry is removed immediately, not after invalidation batch.
 */
class InvalidateNearCacheForKey implements Callable<Boolean>, HazelcastInstanceAware, IdentifiedDataSerializable {
    private String mapName;
    private long[] keys;
    private transient byte[] bytes;
    private transient HazelcastInstance hazelcastInstance;

    InvalidateNearCacheForKey() {
    }

    public InvalidateNearCacheForKey(String mapName, long key, byte[] bytes) {
        this(mapName, new long[]{key}, bytes);
    }
    public InvalidateNearCacheForKey(String mapName, long[] keys, byte[] bytes) {
        this.mapName = mapName;
        this.keys = keys;
        this.bytes = bytes;
    }

    @Override
    public Boolean call() throws Exception {
        IMap<Long, byte[]> map = hazelcastInstance.getMap(mapName);
        if (keys.length > 1) {
            var entries = new HashMap<Long, byte[]>(keys.length);
            for (var key : keys) {
                entries.put(key, bytes);
            }
            map.putAll(entries);
        } else {
            map.set(keys[0], bytes);
        }
        return map instanceof NearCachedMapProxyImpl;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public int getFactoryId() {
        return IdentifiedDataSerializableFactory.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return IdentifiedDataSerializableFactory.INVALIDATE_NEARCACHE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
        out.writeLongArray(keys);
        out.writeInt(bytes.length);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readString();
        keys = in.readLongArray();
        // actual value does not matter, so do not send it. only length is important
        bytes = new byte[in.readInt()];
    }
}
