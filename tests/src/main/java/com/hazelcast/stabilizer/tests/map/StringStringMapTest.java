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

import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.probes.probes.IntervalProbe;
import com.hazelcast.stabilizer.probes.probes.SimpleProbe;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.test.annotations.RunWithWorker;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Teardown;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.tests.helpers.KeyLocality;
import com.hazelcast.stabilizer.tests.helpers.KeyUtils;
import com.hazelcast.stabilizer.tests.helpers.StringUtils;
import com.hazelcast.stabilizer.worker.selector.OperationSelectorBuilder;
import com.hazelcast.stabilizer.worker.tasks.AbstractWorkerTask;

import java.util.Random;

import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.waitClusterSize;

public class StringStringMapTest {

    private static final ILogger log = Logger.getLogger(StringStringMapTest.class);

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public int keyLength = 10;
    public int valueLength = 10;
    public int keyCount = 10000;
    public int valueCount = 10000;
    public String basename = "stringStringMap";
    public KeyLocality keyLocality = KeyLocality.Random;
    public int minNumberOfMembers = 0;

    public double putProb = 0.1;
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
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        map = testContext.getTargetInstance().getMap(basename + "-" + testContext.getTestId());

        operationSelectorBuilder.addOperation(Operation.PUT, putProb).addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
        log.info(getOperationCountInformation(testContext.getTargetInstance()));
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        waitClusterSize(log, testContext.getTargetInstance(), minNumberOfMembers);
        keys = KeyUtils.generateStringKeys(keyCount, keyLength, keyLocality, testContext.getTargetInstance());
        values = StringUtils.generateStrings(valueCount, valueLength);

        Random random = new Random();
        for (String key : keys) {
            String value = values[random.nextInt(valueCount)];
            map.put(key, value);
        }
    }

    @RunWithWorker
    public AbstractWorkerTask<Operation> createBaseWorker() {
        return new WorkerTask();
    }

    private class WorkerTask extends AbstractWorkerTask<Operation> {

        public WorkerTask() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void doRun(Operation operation) {
            String key = randomKey();

            switch (operation) {
                case PUT:
                    String value = randomValue();
                    putLatency.started();
                    if (useSet) {
                        map.set(key, value);
                    } else {
                        map.put(key, value);
                    }
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
            return keys[nextInt(keys.length)];
        }

        private String randomValue() {
            return values[nextInt(values.length)];
        }
    }

    public static void main(String[] args) throws Throwable {
        StringStringMapTest test = new StringStringMapTest();
        new TestRunner<StringStringMapTest>(test).run();
    }
}
