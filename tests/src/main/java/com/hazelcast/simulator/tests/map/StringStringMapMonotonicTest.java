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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
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
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.metronome.MetronomeType;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorkerWithMultipleProbes;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateStrings;
import static com.hazelcast.simulator.worker.metronome.MetronomeFactory.withFixedIntervalMs;

public class StringStringMapMonotonicTest {
    private static final ILogger LOGGER = Logger.getLogger(StringStringMapMonotonicTest.class);

    private enum Operation {
        PUT,
        SET,
        GET
    }

    // properties
    public String basename = StringStringMapTest.class.getSimpleName();
    public int keyCount = 10000;
    public int valueCount = 10000;
    public int keyLength = 10;
    public int valueLength = 10;
    public KeyLocality keyLocality = KeyLocality.SHARED;
    public int minNumberOfMembers = 0;
    public double putProb = 0.1;
    public double setProb = 0.0;
    public int intervalMs = 1;
    public MetronomeType metronomeType = MetronomeType.SLEEPING;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private HazelcastInstance targetInstance;
    private IMap<String, String> map;

    private String[] keys;
    private String[] values;

    @Setup
    public void setUp(TestContext testContext) {
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);

        operationSelectorBuilder.addOperation(Operation.PUT, putProb)
                .addOperation(Operation.SET, setProb)
                .addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void tearDown() {
        map.destroy();
        LOGGER.info(getOperationCountInformation(targetInstance));
    }

    @Warmup(global = false)
    public void warmup() {
        waitClusterSize(LOGGER, targetInstance, minNumberOfMembers);
        keys = generateStringKeys(keyCount, keyLength, keyLocality, targetInstance);
        values = generateStrings(valueCount, valueLength);

        loadInitialData();
    }

    private void loadInitialData() {
        Random random = new Random();
        Streamer<String, String> streamer = StreamerFactory.getInstance(map);
        for (String key : keys) {
            String value = values[random.nextInt(valueCount)];
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker(operationSelectorBuilder);
    }

    private class Worker extends AbstractWorkerWithMultipleProbes<Operation> {

        private final Metronome metronome;

        public Worker(OperationSelectorBuilder<Operation> operationSelectorBuilder) {
            super(operationSelectorBuilder);
            this.metronome = withFixedIntervalMs(intervalMs, metronomeType);
        }

        @Override
        protected void timeStep(Operation operation, Probe probe) throws Exception {
            metronome.waitForNext();

            String key = randomKey();
            long started;

            switch (operation) {
                case PUT:
                    String value = randomValue();
                    started = System.nanoTime();
                    map.put(key, value);
                    probe.done(started);
                    break;
                case SET:
                    value = randomValue();
                    started = System.nanoTime();
                    map.set(key, value);
                    probe.done(started);
                    break;
                case GET:
                    started = System.nanoTime();
                    map.get(key);
                    probe.done(started);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private String randomKey() {
            return keys[randomInt(keys.length)];
        }

        private String randomValue() {
            return values[randomInt(values.length)];
        }
    }

    public static void main(String[] args) throws Exception {
        StringStringMapMonotonicTest test = new StringStringMapMonotonicTest();
        new TestRunner<StringStringMapMonotonicTest>(test).run();
    }
}
