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
package com.hazelcast.stabilizer.tests.concurrent.atomicreference;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.map.helpers.StringUtils;
import com.hazelcast.stabilizer.tests.utils.KeyLocality;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.tests.map.helpers.StringUtils.generateString;
import static com.hazelcast.stabilizer.tests.utils.TestUtils.randomByteArray;
import static com.hazelcast.stabilizer.tests.utils.TestUtils.warmupPartitions;

public class AtomicReferenceTest {

    private final static ILogger log = Logger.getLogger(AtomicReferenceTest.class);

    //props
    public int countersLength = 1000;
    public int threadCount = 10;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 1000;
    public String basename = "atomicreference";
    public KeyLocality keyLocality = KeyLocality.Random;
    public int writePercentage = 100;
    public int valueCount = 1000;
    public int valueLength = 512;
    public boolean useStringValue = true;

    private IAtomicReference[] counters;
    private AtomicLong operations = new AtomicLong();
    private TestContext context;
    private Object[] values;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext context) throws Exception {
        this.context = context;

        if (writePercentage < 0) {
            throw new IllegalArgumentException("Write percentage can't be smaller than 0");
        }

        if (writePercentage > 100) {
            throw new IllegalArgumentException("Write percentage can't be larger than 100");
        }

        targetInstance = context.getTargetInstance();
        warmupPartitions(log, targetInstance);

        values = new Object[valueCount];
        Random random = new Random();
        for (int k = 0; k < valueCount; k++) {
            if (useStringValue) {
                values[k] = generateString(valueLength);
            } else {
                values[k] = randomByteArray(random, valueLength);
            }
        }

        counters = new IAtomicReference[countersLength];
        for (int k = 0; k < counters.length; k++) {
            String key = StringUtils.generateKey(8, keyLocality, targetInstance);
            IAtomicReference atomicReference = targetInstance.getAtomicReference(key);
            atomicReference.set(values[random.nextInt(values.length)]);
            counters[k] = atomicReference;
        }
    }

    @Teardown
    public void teardown() throws Exception {
        for (IAtomicReference counter : counters) {
            counter.destroy();
        }
        log.info(TestUtils.getOperationCountInformation(targetInstance));
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(context.getTestId());
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


            while (!context.isStopped()) {
                IAtomicReference counter = getRandomCounter();
                if (shouldWrite(iteration)) {
                    Object value = values[random.nextInt(values.length)];
                    counter.set(value);
                } else {
                    counter.get();
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

        private boolean shouldWrite(long iteration) {
            if (writePercentage == 0) {
                return false;
            } else if (writePercentage == 100) {
                return true;
            } else {
                return (iteration % 100) < writePercentage;
            }
        }

        private IAtomicReference getRandomCounter() {
            int index = random.nextInt(counters.length);
            return counters[index];
        }
    }

    public static void main(String[] args) throws Throwable {
        AtomicReferenceTest test = new AtomicReferenceTest();
        new TestRunner(test).withDuration(10).run();
    }
}

