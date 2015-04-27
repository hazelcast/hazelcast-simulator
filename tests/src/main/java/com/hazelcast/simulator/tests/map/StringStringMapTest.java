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

import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.probes.probes.SimpleProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.loadsupport.MapStreamer;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateStrings;

public class StringStringMapTest {

    private static final ILogger LOGGER = Logger.getLogger(StringStringMapTest.class);

    private enum Operation {
        PUT,
        SET,
        GET
    }

    // properties
    public int keyLength = 10;
    public int valueLength = 10;
    public int keyCount = 10000;
    public int valueCount = 10000;
    public String basename = "stringStringMap";
    public KeyLocality keyLocality = KeyLocality.RANDOM;
    public int minNumberOfMembers = 0;

    public double putProb = 0.1;
    public double setProb = 0;
    @Deprecated
    // use the setProb property instead
    public boolean useSet = false;

    // probes
    public IntervalProbe putLatency;
    public IntervalProbe getLatency;
    public SimpleProbe throughput;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private TestContext testContext;
    private IMap<String, String> map;

    private String[] keys;
    private String[] values;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        if (useSet) {
            throw new IllegalArgumentException("The 'useSet' property is no longer supported. " +
                    "Configure 'setProb' property to use IMap::set.");
        }

        this.testContext = testContext;
        map = testContext.getTargetInstance().getMap(basename + "-" + testContext.getTestId());

        operationSelectorBuilder.addOperation(Operation.PUT, putProb)
                .addOperation(Operation.SET, setProb)
                .addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void tearDown() throws Exception {
        map.destroy();
        LOGGER.info(getOperationCountInformation(testContext.getTargetInstance()));
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        waitClusterSize(LOGGER, testContext.getTargetInstance(), minNumberOfMembers);
        keys = generateStringKeys(keyCount, keyLength, keyLocality, testContext.getTargetInstance());
        values = generateStrings(valueCount, valueLength);

        loadInitialData();
    }

    private void loadInitialData() throws InterruptedException {
        Random random = new Random();
        MapStreamer<String, String> streamer = new MapStreamer<String, String>(map);
        for (String key : keys) {
            String value = values[random.nextInt(valueCount)];
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
            String key = randomKey();

            switch (operation) {
                case PUT:
                    String value = randomValue();
                    putLatency.started();
                    map.put(key, value);
                    putLatency.done();
                    break;
                case SET:
                    value = randomValue();
                    putLatency.started();
                    map.set(key, value);
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

            throughput.done();
        }

        private String randomKey() {
            return keys[randomInt(keys.length)];
        }

        private String randomValue() {
            return values[randomInt(values.length)];
        }
    }

    public static void main(String[] args) throws Exception {
        StringStringMapTest test = new StringStringMapTest();
        new TestRunner<StringStringMapTest>(test).run();
    }
}
