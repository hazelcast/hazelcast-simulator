/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * A test that checks how fast externalizable values can be put in a map. The key is an integer.
 *
 * todo:
 * add the following serializer
 * - identified data-serializable
 * - kryo
 * - fast-serialization
 */
public class MapSerializationTest {

    public enum Serializer {
        Serializable,
        Externalizable,
        DataSerializable,
        Long
    }

    // properties
    public String basename = MapSerializationTest.class.getSimpleName();
    public int threadCount = 10;
    public int keyCount = 1000000;
    public Serializer serializer = Serializer.Serializable;

    // probes
    public Probe probe;

    private IMap<Integer, Object> map;

    @Setup
    public void setUp(TestContext testContext) {
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
        map = hazelcastInstance.getMap(basename);
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {
        @Override
        protected void timeStep() throws Exception {
            int key = randomInt(keyCount);
            switch (serializer) {
                case Serializable:
                    map.put(key, new SerializableValue(key));
                    break;
                case Externalizable:
                    map.put(key, new ExternalizableValue(key));
                    break;
                case DataSerializable:
                    map.put(key, new DataSerializableValue(key));
                    break;
                case Long:
                    map.put(key, new Long(key));
                    break;
                default:
                    throw new IllegalStateException("Unrecognized serializer: " + serializer);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        MapSerializationTest test = new MapSerializationTest();
        new TestRunner<MapSerializationTest>(test).run();
    }

    public static class ExternalizableValue implements Externalizable {
        private int v;

        public ExternalizableValue() {
        }

        public ExternalizableValue(int v) {
            this.v = v;
        }

        public int id() {
            return v;
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            v = in.readInt();
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(v);
        }
    }

    public static class SerializableValue implements Serializable {
        private int v;

        public SerializableValue(int v) {
            this.v = v;
        }

        public int id() {
            return v;
        }
    }


    public static class DataSerializableValue implements DataSerializable {
        private int v;

        public DataSerializableValue() {
        }


        public DataSerializableValue(int v) {
            this.v = v;
        }

        public int id() {
            return v;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(v);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            v = in.readInt();
        }
    }
}
