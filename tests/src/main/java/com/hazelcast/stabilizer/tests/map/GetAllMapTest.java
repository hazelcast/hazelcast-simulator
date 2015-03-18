/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;

import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.test.annotations.*;
import com.hazelcast.stabilizer.worker.selector.OperationSelectorBuilder;
import com.hazelcast.stabilizer.worker.tasks.AbstractWorker;

import java.util.Random;

import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.getOperationCountInformation;


public class GetAllMapTest {

    private static final ILogger log = Logger.getLogger(GetAllMapTest.class);
    private static final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private enum Operation {
        PUT,
        GET,
    }

    // properties
    public int threadCount = 10;
    public int keyCount = 1000;


    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private String basename = "tempVALE";
    private IMap<Object, Object> map;

    @Setup
    public void setup(TestContext testContext) throws Exception {

        operationSelectorBuilder.addOperation(Operation.PUT, 0.5)
                                .addOperation(Operation.GET, 0.5);


        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

        map = testContext.getTargetInstance().getMap(basename);
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
        log.info(getOperationCountInformation(testContext.getTargetInstance()));
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException { }

    @RunWithWorker
    public AbstractWorker<Operation> createWorker() {
        return new WorkerTask();
    }

    private class WorkerTask extends AbstractWorker<Operation> {


        Random random = new Random();

        public WorkerTask() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void timeStep(Operation operation) {
            int key = random.nextInt(keyCount);

            switch (operation) {
                case PUT:
                    map.put(key, key+"HI");

                    break;
                case GET:
                    map.get(key);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }

        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        log.info("map name = "+map.getName());
        log.info("map size = "+map.size());
    }

    public static void main(String[] args) throws Throwable {
        GetAllMapTest test = new GetAllMapTest();
        new TestRunner<GetAllMapTest>(test).run();
    }
}
