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
package ${package};

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import static org.junit.Assert.assertEquals;

public class ExampleTest {

    private enum Operation {
        PUT,
        GET
    }

    private static final ILogger LOGGER = Logger.getLogger(ExampleTest.class);

    // properties
    public int maxKeys = 1000;
    public double putProb = 0.5;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private IMap<Integer, String> map;

    @Setup
    public void setUp(TestContext testContext) {
        LOGGER.info("======== SETUP =========");
        HazelcastInstance targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap("exampleMap");

        LOGGER.info("Map name is: " + map.getName());

        operationSelectorBuilder
                .addOperation(Operation.PUT, putProb)
                .addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void tearDown() {
        LOGGER.info("======== TEAR DOWN =========");
        map.destroy();
        LOGGER.info("======== THE END =========");
    }

    @Warmup
    public void warmup() {
        LOGGER.info("======== WARMUP =========");
        LOGGER.info("Map size is: " + map.size());
    }

    @Verify
    public void verify() {
        LOGGER.info("======== VERIFYING =========");
        LOGGER.info("Map size is: " + map.size());

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

    private class Worker extends AbstractWorker<Operation> {

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            int key = randomInt(maxKeys);
            switch (operation) {
                case PUT:
                    map.put(key, "value" + key);
                    break;
                case GET:
                    map.get(key);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown operation: " + operation);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ExampleTest test = new ExampleTest();
        new TestRunner<ExampleTest>(test).run();
    }
}
