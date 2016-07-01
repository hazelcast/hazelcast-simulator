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
package com.hazelcast.simulator.tests;

import com.hazelcast.core.IMap;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorkerWithMultipleProbes;

import static org.junit.Assert.assertEquals;

public class ExampleTest extends AbstractTest {

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public int maxKeys = 1000;
    public double putProb = 0.5;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private IMap<Integer, String> map;

    @Setup
    public void setUp() {
        logger.info("======== SETUP =========");
        map = targetInstance.getMap("exampleMap");

        logger.info("Map name is: " + map.getName());

        operationSelectorBuilder
                .addOperation(Operation.PUT, putProb)
                .addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void tearDown() {
        logger.info("======== TEAR DOWN =========");
        map.destroy();
        logger.info("======== THE END =========");
    }

    @Warmup
    public void warmup() {
        logger.info("======== WARMUP =========");
        logger.info("Map size is: " + map.size());
    }

    @Verify
    public void verify() {
        logger.info("======== VERIFYING =========");
        logger.info("Map size is: " + map.size());

        for (int i = 0; i < maxKeys; i++) {
            String actualValue = map.get(i);
            if (actualValue != null) {
                String expectedValue = "value" + i;
                assertEquals(expectedValue, actualValue);
            }
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
            int key = randomInt(maxKeys);
            long started;

            switch (operation) {
                case PUT:
                    started = System.nanoTime();
                    map.put(key, "value" + key);
                    probe.done(started);
                    break;
                case GET:
                    started = System.nanoTime();
                    map.get(key);
                    probe.done(started);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown operation: " + operation);
            }
        }
    }
}
