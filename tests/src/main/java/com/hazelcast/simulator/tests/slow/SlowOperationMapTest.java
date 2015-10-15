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
package com.hazelcast.simulator.tests.slow;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;
import com.hazelcast.simulator.worker.tasks.IWorker;
import com.hazelcast.simulator.worker.tasks.NoOperationWorker;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationService;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isClient;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.ReflectionUtils.getFieldValue;
import static com.hazelcast.simulator.utils.TestUtils.assertEqualsStringFormat;
import static java.lang.String.format;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test invokes slowed down map operations on a Hazelcast instance to provoke slow operation logs.
 *
 * In the verification phase we check for the correct number of slow operation logs (one per operation type).
 *
 * @since Hazelcast 3.5
 */
public class SlowOperationMapTest {

    private static final ILogger LOGGER = Logger.getLogger(SlowOperationMapTest.class);

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public String basename = SlowOperationMapTest.class.getSimpleName();
    public int keyLength = 10;
    public int valueLength = 10;
    public int keyCount = 100;
    public int valueCount = 100;
    public double putProb = 0.5;
    public int recursionDepth = 10;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();
    private final AtomicLong putCounter = new AtomicLong(0);
    private final AtomicLong getCounter = new AtomicLong(0);

    private boolean isClient;
    private int[] keys;
    private IMap<Integer, Integer> map;
    private Object slowOperationDetector;

    @Setup
    public void setUp(TestContext testContext) {
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
        isClient = isClient(hazelcastInstance);
        keys = generateIntKeys(keyCount, Integer.MAX_VALUE, KeyLocality.LOCAL, hazelcastInstance);
        map = hazelcastInstance.getMap(basename);

        operationSelectorBuilder
                .addOperation(Operation.PUT, putProb)
                .addDefaultOperation(Operation.GET);

        // try to find the slowOperationDetector instance (since Hazelcast 3.5)
        if (isMemberNode(hazelcastInstance)) {
            slowOperationDetector = getFieldValue(getOperationService(hazelcastInstance), "slowOperationDetector");
            if (slowOperationDetector == null) {
                fail(basename + ": This test needs Hazelcast 3.5 or newer");
            }
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }

    @Warmup
    public void localWarmup() {
        if (isClient) {
            return;
        }
        Random random = new Random();
        for (int key : keys) {
            int value = random.nextInt(Integer.MAX_VALUE);
            map.put(key, value);
        }
    }

    @Warmup(global = true)
    public void globalWarmup() {
        // add the interceptor after local warmup, otherwise that stage will take ages
        map.addInterceptor(new SlowMapInterceptor(recursionDepth));
    }

    @Verify
    public void verify() {
        if (isClient) {
            return;
        }
        long putCount = putCounter.get();
        long getCount = getCounter.get();
        Map<Integer, Object> slowOperationLogs = getFieldValue(slowOperationDetector, "slowOperationLogs");

        int expected = (int) (Math.min(putCount, 1) + Math.min(getCount, 1));
        long operationCount = putCount + getCount;

        LOGGER.info(format("Expecting %d slow operation logs after completing %d operations (%d put, %d get).",
                expected, operationCount, putCount, getCount));

        assertNotNull("Could not retrieve slow operation logs", slowOperationLogs);
        assertEqualsStringFormat("Expected %d slow operation logs, but was %d", expected, slowOperationLogs.size());
        assertTrue("Expected at least one completed operations, but was " + operationCount
                + ". Please run the test for a longer time!", operationCount > 0);
    }

    @RunWithWorker
    public IWorker createWorker() {
        if (isClient) {
            // if clients execute put or get operations we may produce slow operation logs on a member with put/getCounter == 0
            // in that case the verification will fail without reason, so we create a noop worker for clients
            return new NoOperationWorker();
        }
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
                    map.put(key, randomValue());
                    putCounter.incrementAndGet();
                    break;
                case GET:
                    map.get(key);
                    getCounter.incrementAndGet();
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

    private static class SlowMapInterceptor implements MapInterceptor {
        private final int recursionDepth;

        public SlowMapInterceptor(int recursionDepth) {
            this.recursionDepth = recursionDepth;
        }

        @Override
        public Object interceptGet(Object value) {
            return null;
        }

        @Override
        public void afterGet(Object value) {
            sleepRecursion(recursionDepth, 15);
        }

        @Override
        public Object interceptPut(Object oldValue, Object newValue) {
            return null;
        }

        @Override
        public void afterPut(Object value) {
            sleepRecursion(recursionDepth, 20);
        }

        @Override
        public Object interceptRemove(Object removedValue) {
            return null;
        }

        @Override
        public void afterRemove(Object removedValue) {
        }

        private void sleepRecursion(int recursionDepth, int sleepSeconds) {
            if (recursionDepth == 0) {
                sleepSeconds(sleepSeconds);
                return;
            }
            sleepRecursion(recursionDepth - 1, sleepSeconds);
        }
    }

    public static void main(String[] args) throws Exception {
        SlowOperationMapTest test = new SlowOperationMapTest();
        new TestRunner<SlowOperationMapTest>(test).run();
    }
}
