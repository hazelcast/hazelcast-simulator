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

import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.StartNanos;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Level;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import static java.lang.Math.abs;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class MultiValueMapTest extends HazelcastTest {

    public int keyCount = 100000;
    public int maxNestedValues = 100;
    public boolean useIndex;
    public boolean usePortable;

    private final ThrottlingLogger throttlingLogger = ThrottlingLogger.newLogger(logger, 5000);
    private IMap<Integer, Object> map;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
    }

    @Prepare(global = true)
    public void prepare() {
        if (useIndex) {
            map.addIndex("payloadField[any]", true);
        }
        loadInitialData();
    }

    private void loadInitialData() {
        Streamer<Integer, Object> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < keyCount; i++) {
            int count = i % maxNestedValues;
            SillySequence sillySequence = new SillySequence(i, count);
            streamer.pushEntry(i, sillySequence);
        }
        streamer.await();
    }

    @TimeStep(prob = 0.5)
    public void put(ThreadState state) {
        int key = state.getRandomKey();
        int count = key % maxNestedValues;
        SillySequence sillySequence = new SillySequence(key, count);
        map.put(key, usePortable ? sillySequence.getPortable() : sillySequence);
    }

    @TimeStep(prob = -1)
    public void query(ThreadState state, Probe probe, @StartNanos long startNanos) {
        int key = state.getRandomKey();
        Predicate predicate = Predicates.equal("payloadField[any]", key);
        Collection<Object> result = null;
        try {
            result = map.values(predicate);
        } finally {
            probe.done(startNanos);
        }

        if (throttlingLogger.requestLogSlot()) {
            throttlingLogger.logInSlot(Level.INFO,
                    format("Query 'payloadField[any]= %d' returned %d results.", key, result.size()));
        }

        for (Object resultSillySequence : result) {
            state.assertValidSequence(resultSillySequence);
        }
    }

    public class ThreadState extends BaseThreadState {

        private int getRandomKey() {
            return abs(randomInt(keyCount));
        }

        private void assertValidSequence(Object sillySequenceObject) {
            Collection<Integer> payload;
            if (sillySequenceObject instanceof SillySequencePortable) {
                SillySequencePortable ssp = (SillySequencePortable) sillySequenceObject;
                payload = asList(ArrayUtils.toObject(ssp.payloadField));
                assertEquals(ssp.count, payload.size());
            } else {
                SillySequence ss = (SillySequence) sillySequenceObject;
                payload = ss.payloadField;
                assertEquals(ss.count, payload.size());
            }


            Integer lastValue = null;
            for (int i : payload) {
                if (lastValue != null) {
                    int expectedValue = lastValue + 1;
                    assertEquals(expectedValue, i);
                }
                lastValue = i;
            }
        }
    }

    private static class SillySequence implements DataSerializable {

        int count;
        Collection<Integer> payloadField;

        @SuppressWarnings("unused")
        SillySequence() {
        }

        SillySequence(int from, int count) {
            this.count = count;
            this.payloadField = new ArrayList<Integer>(count);

            int to = from + count;
            for (int i = from; i < to; i++) {
                payloadField.add(i);
            }
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(count);
            out.writeObject(payloadField);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            count = in.readInt();
            payloadField = in.readObject();
        }

        public Object getPortable() {
            SillySequencePortable portable = new SillySequencePortable();
            portable.count = this.count;
            portable.payloadField = ArrayUtils.toPrimitive(payloadField.toArray(new Integer[payloadField.size()]));
            return portable;
        }
    }

    private static class SillySequencePortable implements Portable {
        int count;
        int[] payloadField;

        @SuppressWarnings("unused")
        SillySequencePortable() {
        }

        @Override
        public int getFactoryId() {
            return SillySequencePortableFactory.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writePortable(PortableWriter out) throws IOException {
            out.writeInt("count", count);
            out.writeIntArray("payloadField", payloadField);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            count = reader.readInt("count");
            payloadField = reader.readIntArray("payloadField");
        }
    }

    public static final class SillySequencePortableFactory implements PortableFactory {

        public static final int FACTORY_ID = 5001;

        @Override
        public Portable create(int i) {
            return new SillySequencePortable();
        }
    }
}
