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
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.map.helpers.KeyUtils;
import com.hazelcast.stabilizer.tests.map.helpers.StringUtils;
import com.hazelcast.stabilizer.tests.utils.KeyLocality;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import com.hazelcast.stabilizer.worker.Metronome;
import com.hazelcast.stabilizer.worker.SimpleMetronome;

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
    public KeyLocality keyLocality = KeyLocality.Random;
    public int minNumberOfMembers = 0;
    private int intervalMs;

    //probes
    public IntervalProbe getLatency;
    public IntervalProbe putLatency;

    public SimpleProbe throughput;
    private IMap<String, String> map;
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
        log.info(TestUtils.getOperationCountInformation(targetInstance));
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        TestUtils.waitClusterSize(log, targetInstance, minNumberOfMembers);
        keys = KeyUtils.generateKeys(keyCount, keyLength, keyLocality, testContext.getTargetInstance());
        values = StringUtils.generateStrings(valueCount, valueLength);

        Random random = new Random();
        for (int k = 0; k < keys.length; k++) {
            String key = keys[k];
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

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            Metronome metronome = SimpleMetronome.withFixedIntervalMs(intervalMs);
            while (!testContext.isStopped()) {
                metronome.waitForNext();
                String key = randomKey();

                if (shouldWrite(iteration)) {
                    String value = randomValue();
                    putLatency.started();
                    if (usePut) {
                        map.put(key, value);
                    } else {
                        map.set(key, value);
                    }
                    putLatency.done();
                } else {
                    getLatency.started();
                    map.get(key);
                    getLatency.done();
                }

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }

                iteration++;
                throughput.done();
            }
        }

        private String randomValue() {
            return values[random.nextInt(values.length)];
        }

        private String randomKey() {
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
