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
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.map.helpers.StringUtils;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class StringMapTest {

    private final static ILogger log = Logger.getLogger(StringMapTest.class);

    //props
    public int writePercentage = 10;
    public int threadCount = 10;
    public int keyLength = 10;
    public int valueLength = 10;
    public int keyCount = 10000;
    public int valueCount = 10000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    public boolean usePut = true;
    public String basename = "stringmap";
    public boolean preventLocalCalls = false;
    public int minNumberOfMembers = 0;

    private IMap<Object, Object> map;
    private String[] keys;
    private String[] values;
    private final AtomicLong operations = new AtomicLong();
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        if (writePercentage < 0) {
            throw new IllegalArgumentException("Write percentage can't be smaller than 0");
        }

        if (writePercentage > 100) {
            throw new IllegalArgumentException("Write percentage can't be larger than 100");
        }

        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename + "-" + testContext.getTestId());
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        TestUtils.waitClusterSize(log, targetInstance, minNumberOfMembers);
        warmUpPartitions(testContext.getTargetInstance());
        keys = new String[keyCount];
        for (int k = 0; k < keys.length; k++) {
            keys[k] = StringUtils.generateKey(keyLength, preventLocalCalls, testContext.getTargetInstance());
        }

        values = new String[valueCount];
        for (int k = 0; k < values.length; k++) {
            values[k] = StringUtils.makeString(valueLength);
        }

        Random random = new Random();

        for (int k = 0; k < keys.length; k++) {
            String key = keys[random.nextInt(keyCount)];
            String value = values[random.nextInt(valueCount)];
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

    public void warmUpPartitions(HazelcastInstance... instances) throws InterruptedException {
        for (HazelcastInstance instance : instances) {
            final PartitionService ps = instance.getPartitionService();
            for (Partition partition : ps.getPartitions()) {
                while (partition.getOwner() == null) {
                    Thread.sleep(10);
                }
            }
        }
        for (int i = 0; i < 100000; i++) {
            map.get(i);
        }
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

                Object key = randomKey();

                if (shouldWrite(iteration)) {
                    Object value = randomValue();
                    if (usePut) {
                        map.put(key, value);
                    } else {
                        map.set(key, value);
                    }
                } else {
                    map.get(key);
                }

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }

                iteration++;
            }
        }

        private Object randomValue() {
            return values[random.nextInt(values.length)];
        }

        private Object randomKey() {
            int length = keys.length;
            return keys[random.nextInt(length)];
        }

        private boolean shouldWrite(long iteration) {
            if (writePercentage == 0) {
                return false;
            } else if (writePercentage == 100) {
                return true;
            } else {
                return (iteration % 100) < writePercentage;
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        StringMapTest test = new StringMapTest();
        test.writePercentage = 10;
        new TestRunner(test).run();
    }
}
