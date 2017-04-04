/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * A test that checks how fast externalizable values can be put in a map. The key is an integer.
 *
 * TODO:
 * add the following serializer
 * - identified data-serializable
 * - kryo
 * - fast-serialization
 */
public class MapSerializationTest extends HazelcastTest {

    private enum Serializer {
        SERIALIZABLE,
        EXTERNALIZABLE,
        DATA_SERIALIZABLE,
        LONG
    }

    // properties
    public int keyCount = 1000000;
    public Serializer serializer = Serializer.SERIALIZABLE;

    private IMap<Integer, Object> map;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
    }

    @TimeStep
    public void timeStep(BaseThreadState state) throws Exception {
        int key = state.randomInt(keyCount);

        //todo: it would be better to have multiple timestep methods so that optimized code can be generated.
        switch (serializer) {
            case SERIALIZABLE:
                map.put(key, new SerializableValue(key));
                break;
            case EXTERNALIZABLE:
                map.put(key, new ExternalizableValue(key));
                break;
            case DATA_SERIALIZABLE:
                map.put(key, new DataSerializableValue(key));
                break;
            case LONG:
                map.put(key, (long) key);
                break;
            default:
                throw new IllegalStateException("Unrecognized serializer: " + serializer);
        }
    }

    private static class ExternalizableValue implements Externalizable {

        private int value;

        public ExternalizableValue() {
        }

        ExternalizableValue(int value) {
            this.value = value;
        }

        public int id() {
            return value;
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            value = in.readInt();
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(value);
        }
    }

    private static class SerializableValue implements Serializable {

        private int value;

        SerializableValue(int value) {
            this.value = value;
        }

        public int id() {
            return value;
        }
    }

    private static class DataSerializableValue implements DataSerializable {

        private int value;

        @SuppressWarnings("unused")
        public DataSerializableValue() {
        }

        DataSerializableValue(int value) {
            this.value = value;
        }

        public int id() {
            return value;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(value);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            value = in.readInt();
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
