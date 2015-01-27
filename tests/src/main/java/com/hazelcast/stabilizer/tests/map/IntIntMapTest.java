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
import com.hazelcast.stabilizer.probes.probes.IntervalProbe;
import com.hazelcast.stabilizer.probes.probes.SimpleProbe;
import com.hazelcast.stabilizer.tests.helpers.KeyLocality;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.test.annotations.Performance;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Teardown;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.tests.helpers.KeyUtils;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;
import com.hazelcast.stabilizer.worker.OperationSelector;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.stabilizer.tests.map.IntIntMapTest.Operation.*;

public class IntIntMapTest {

    private final static ILogger log = Logger.getLogger(IntIntMapTest.class);

    // properties
    public int threadCount = 10;
    public int keyLength = 10;
    public int valueLength = 10;
    public int keyCount = 10000;
    public int valueCount = 10000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    public String basename = "intIntMap";
    public KeyLocality keyLocality = KeyLocality.Random;
    public int minNumberOfMembers = 0;

    public double putProb = 0.1;
    public boolean useSet = false;

    // probes
    public IntervalProbe putLatency;
    public IntervalProbe getLatency;
    public SimpleProbe throughput;

    private IMap<Integer, Integer> map;
    private int[] keys;
    private final AtomicLong operations = new AtomicLong();
    private TestContext testContext;

    private HazelcastInstance targetInstance;

    private OperationSelector<Operation> selector = new OperationSelector<Operation>();

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename + "-" + testContext.getTestId());

        selector.addOperation(PUT, putProb)
                .addOperationRemainingProbability(GET);
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
        log.info(getOperationCountInformation(targetInstance));
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        waitClusterSize(log, targetInstance, minNumberOfMembers);
        keys = KeyUtils.generateIntKeys(keyCount, Integer.MAX_VALUE, keyLocality, testContext.getTargetInstance());

        Random random = new Random();
        for (int key : keys) {
            int value = random.nextInt(Integer.MAX_VALUE);
            map.put(key, value);
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            while (!testContext.isStopped()) {

                int key = randomKey();
                int value = randomValue();

                switch (selector.select()) {
                    case PUT:
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
                        map.put(key, value);
                        getLatency.done();
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }

                iteration++;
                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }

                throughput.done();
            }
            operations.addAndGet(iteration % performanceUpdateFrequency);
        }

        private int randomKey() {
            int length = keys.length;
            return keys[random.nextInt(length)];
        }

        private int randomValue() {
            return random.nextInt(Integer.MAX_VALUE);
        }
    }

    public static void main(String[] args) throws Throwable {
        IntIntMapTest test = new IntIntMapTest();
        new TestRunner<IntIntMapTest>(test).run();
    }

    static enum Operation {
        PUT,
        GET
    }
}
