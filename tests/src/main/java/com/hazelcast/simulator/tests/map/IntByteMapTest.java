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
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.loadsupport.MapStreamer;
import com.hazelcast.simulator.worker.loadsupport.MapStreamerFactory;
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
    public int valueSize = 16;
    public KeyLocality keyLocality = KeyLocality.RANDOM;
    public double putProb = 0.3;

    // probes
    public IntervalProbe putLatency;
    public IntervalProbe getLatency;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private IMap<Integer, Object> map;
    private int[] keys;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        HazelcastInstance instance = testContext.getTargetInstance();
        map = instance.getMap(basename);
        keys = generateIntKeys(keyCount, Integer.MAX_VALUE, keyLocality, instance);

        operationSelectorBuilder
                .addOperation(Operation.PUT, putProb)
                .addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void tearDown() throws Exception {
        map.destroy();
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        MapStreamer<Integer, Object> streamer = MapStreamerFactory.getInstance(map);
        Random random = new Random();
        for (int key : keys) {
            Object value = generateByteArray(random, valueSize);
            streamer.pushEntry(key, value);
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
        protected void timeStep(Operation operation) {
            int key = keys[randomInt(keys.length)];

            switch (operation) {
                case PUT:
                    Object value = generateByteArray(getRandom(), valueSize);
                    putLatency.started();
                    map.put(key, value);
                    putLatency.done();
                    break;
                case GET:
                    getLatency.started();
                    map.get(key);
                    getLatency.done();
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
