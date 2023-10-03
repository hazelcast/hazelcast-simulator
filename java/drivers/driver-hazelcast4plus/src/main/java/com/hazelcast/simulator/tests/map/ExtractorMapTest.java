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

import com.hazelcast.config.IndexType;
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
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.query.extractor.ValueReader;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.probes.LatencyProbe;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.StartNanos;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.Level;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.lang.Math.abs;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class ExtractorMapTest extends HazelcastTest {

    public int keyCount = 100000;
    public int nestedValuesCount = 100;
    public int indexValuesCount = 5;
    public boolean useIndex;
    public boolean usePortable;

    private final ThrottlingLogger throttlingLogger = ThrottlingLogger.newLogger(logger, 5000);
    private IMap<Integer, Object> map;

    @Setup
    public void setUp() {
        String mapName = usePortable ? "Portable " + name : name;
        map = targetInstance.getMap(mapName);
    }

    @Prepare(global = true)
    public void globalPrepare() {
        if (useIndex) {
            for (int i = 0; i < indexValuesCount; i++) {
                map.addIndex(IndexType.SORTED, format("payloadFromExtractor[%d]", i));
            }
        }

        loadInitialData();
    }

    private void loadInitialData() {
        Streamer<Integer, Object> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < keyCount; i++) {
            SillySequence sillySequence = new SillySequence(i, nestedValuesCount);
            streamer.pushEntry(i, usePortable ? sillySequence.getPortable() : sillySequence);
        }
        streamer.await();
    }

    @TimeStep(prob = 0.5)
    public void put(ThreadState state) {
        int key = state.getRandomKey();
        SillySequence sillySequence = new SillySequence(key, nestedValuesCount);
        map.put(key, usePortable ? sillySequence.getPortable() : sillySequence);
    }

    @TimeStep(prob = -1)
    public void query(ThreadState state, LatencyProbe probe, @StartNanos long startedNanos) {
        int key = state.getRandomKey();
        int index = key % nestedValuesCount;
        String query = format("payloadFromExtractor[%d]", index);
        Predicate predicate = Predicates.equal(query, key);
        Collection<Object> result = null;
        try {
            result = map.values(predicate);
        } finally {
            probe.done(startedNanos);
        }

        if (throttlingLogger.requestLogSlot()) {
            throttlingLogger.logInSlot(Level.INFO,
                    format("Query 'payloadFromExtractor[%d]= %d' returned %d results.", index, key, result.size()));
        }

        for (Object resultSillySequence : result) {
            state.assertValidSequence(key, resultSillySequence);
        }
    }

    public class ThreadState extends BaseThreadState {

        private int getRandomKey() {
            return abs(randomInt(keyCount)) % indexValuesCount;
        }

        private void assertValidSequence(Integer key, Object sillySequenceObject) {
            int index = key % nestedValuesCount;
            if (sillySequenceObject instanceof SillySequencePortable) {
                assertEquals(key.intValue(), ((SillySequencePortable) sillySequenceObject).payloadField[index]);
            } else {
                assertEquals(key, ((SillySequence) sillySequenceObject).payloadField.get(index));
            }
        }
    }

    private static class SillySequence implements DataSerializable {
        int count;
        List<Integer> payloadField;

        @SuppressWarnings("unused")
        SillySequence() {
        }

        SillySequence(int from, int count) {
            this.count = count;
            this.payloadField = new ArrayList<>(count);

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

        public Portable getPortable() {
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

    public static final class PayloadExtractor implements ValueExtractor<SillySequence, String> {
        @Override
        public void extract(SillySequence sillySequence, String indexString, ValueCollector valueCollector) {
            valueCollector.addObject(sillySequence.payloadField.get(Integer.parseInt(indexString)));
        }
    }

    public static final class PayloadPortableExtractor implements ValueExtractor<ValueReader, String> {
        @Override
        public void extract(ValueReader reader, String indexString, ValueCollector valueCollector) {
            reader.read("payloadFromExtractor[" + indexString + "]", valueCollector);
        }
    }

    public static final class SillySequencePortableFactory implements PortableFactory {

        public static final int FACTORY_ID = 5000;

        @Override
        public Portable create(int i) {
            return new SillySequencePortable();
        }
    }

}
