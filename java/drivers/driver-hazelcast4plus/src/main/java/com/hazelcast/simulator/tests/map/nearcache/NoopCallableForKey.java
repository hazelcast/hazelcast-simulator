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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Performs no operations - control for estimating executor service overheads.
 * Contains dummy payload to include overheads of serialization.
 */
class NoopCallableForKey implements Callable<Boolean>, HazelcastInstanceAware, IdentifiedDataSerializable {
    private String mapName;
    private long key;
    private transient HazelcastInstance hazelcastInstance;

    NoopCallableForKey() {
    }

    public NoopCallableForKey(String mapName, long key) {
        this.mapName = mapName;
        this.key = key;
    }

    @Override
    public Boolean call() throws Exception {
        return true;
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
        return IdentifiedDataSerializableFactory.NOOP;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
        out.writeLong(key);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readString();
        key = in.readLong();
    }
}
