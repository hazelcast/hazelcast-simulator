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
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;

public class IntByteMapTest {

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public String basename = IntByteMapTest.class.getSimpleName();
    public int keyCount = 1000;
    public int valueCount = 1000;
    public int minSize = 16;
    public int maxSize = 2000;
    public KeyLocality keyLocality = KeyLocality.RANDOM;
    public double putProb = 0.3;

    // probes
    public Probe putProbe;
    public Probe getProbe;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private IMap<Integer, Object> map;
    private int[] keys;
    private byte[][] values;

    @Setup
    public void setUp(TestContext testContext) {
        HazelcastInstance instance = testContext.getTargetInstance();
        map = instance.getMap(basename);
        keys = generateIntKeys(keyCount, Integer.MAX_VALUE, keyLocality, instance);

        if (minSize > maxSize) {
            throw new IllegalStateException("minSize can't be larger than maxSize");
        }

        operationSelectorBuilder
                .addOperation(Operation.PUT, putProb)
                .addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }

    @Warmup(global = false)
    public void warmup() {
        Random random = new Random();
        values = new byte[valueCount][];
        for (int i = 0; i < values.length; i++) {
            int delta = maxSize - minSize;
            int length = delta == 0 ? minSize : minSize + random.nextInt(delta);
            values[i] = generateByteArray(random, length);
        }

        Streamer<Integer, Object> streamer = StreamerFactory.getInstance(map);
        for (int key : keys) {
            streamer.pushEntry(key, values[random.nextInt(values.length)]);
        }
        streamer.await();
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            int key = keys[randomInt(keys.length)];

            switch (operation) {
                case PUT:
                    byte[] value = values[getRandom().nextInt(values.length)];
                    putProbe.started();
                    map.put(key, value);
                    putProbe.done();
                    break;
                case GET:
                    getProbe.started();
                    map.get(key);
                    getProbe.done();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        IntByteMapTest test = new IntByteMapTest();
        new TestRunner<IntByteMapTest>(test).withDuration(10).run();
    }
}
