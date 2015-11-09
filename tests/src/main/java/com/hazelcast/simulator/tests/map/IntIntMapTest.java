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

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;

public class IntIntMapTest {

    private static final ILogger LOGGER = Logger.getLogger(IntIntMapTest.class);

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public String basename = IntIntMapTest.class.getSimpleName();
    public int keyCount = 10000;
    public int valueCount = 10000;
    public int keyLength = 10;
    public int valueLength = 10;
    public KeyLocality keyLocality = KeyLocality.RANDOM;
    public int minNumberOfMembers = 0;

    public double putProb = 0.1;
    public boolean useSet = false;

    // probes
    public Probe putProbe;
    public Probe getProbe;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private TestContext testContext;
    private IMap<Integer, Integer> map;

    private int[] keys;

    @Setup
    public void setUp(TestContext testContext) {
        this.testContext = testContext;
        map = testContext.getTargetInstance().getMap(basename);

        operationSelectorBuilder.addOperation(Operation.PUT, putProb).addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void tearDown() {
        map.destroy();
        LOGGER.info(getOperationCountInformation(testContext.getTargetInstance()));
    }

    @Warmup(global = false)
    public void warmup() {
        waitClusterSize(LOGGER, testContext.getTargetInstance(), minNumberOfMembers);
        keys = generateIntKeys(keyCount, Integer.MAX_VALUE, keyLocality, testContext.getTargetInstance());
        Streamer<Integer, Integer> streamer = StreamerFactory.getInstance(map);
        Random random = new Random();
        for (int key : keys) {
            int value = random.nextInt(Integer.MAX_VALUE);
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
        protected void timeStep(Operation operation) throws Exception {
            int key = randomKey();

            switch (operation) {
                case PUT:
                    int value = randomValue();
                    putProbe.started();
                    if (useSet) {
                        map.set(key, value);
                    } else {
                        map.put(key, value);
                    }
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

        private int randomKey() {
            return keys[randomInt(keys.length)];
        }

        private int randomValue() {
            return randomInt(Integer.MAX_VALUE);
        }
    }

    public static void main(String[] args) throws Exception {
        IntIntMapTest test = new IntIntMapTest();
        new TestRunner<IntIntMapTest>(test).withDuration(10).run();
    }
}
