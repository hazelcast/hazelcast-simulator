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
package com.hazelcast.simulator.tests.syntheticmap;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorkerWithMultipleProbes;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateStrings;

public class StringStringSyntheticMapTest extends AbstractTest {

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public int keyLength = 10;
    public int valueLength = 10;
    public int keyCount = 10000;
    public int valueCount = 10000;
    public KeyLocality keyLocality = KeyLocality.SHARED;
    public int minNumberOfMembers = 0;

    public double putProb = 0.1;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private SyntheticMap<String, String> map;

    private String[] keys;
    private String[] values;

    @Setup
    public void setUp() {
        map = targetInstance.getDistributedObject(SyntheticMapService.SERVICE_NAME, "map-" + name);

        operationSelectorBuilder
                .addOperation(Operation.PUT, putProb)
                .addDefaultOperation(Operation.GET);
    }
    @Warmup
    public void warmup() {
        waitClusterSize(logger, targetInstance, minNumberOfMembers);
        keys = generateStringKeys(keyCount, keyLength, keyLocality, targetInstance);
        values = generateStrings(valueCount, valueLength);

        Random random = new Random();
        for (String key : keys) {
            String value = values[random.nextInt(valueCount)];
            map.put(key, value);
        }
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorkerWithMultipleProbes<Operation> {

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void timeStep(Operation operation, Probe probe) throws Exception {
            String key = randomKey();
            long started;

            switch (operation) {
                case PUT:
                    String value = randomValue();
                    started = System.nanoTime();
                    map.put(key, value);
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


    @Teardown
    public void tearDown() {
        map.destroy();
        logger.info(getOperationCountInformation(targetInstance));
    }


}
