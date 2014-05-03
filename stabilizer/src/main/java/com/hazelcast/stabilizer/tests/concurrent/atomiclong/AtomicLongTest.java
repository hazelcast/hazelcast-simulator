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
package com.hazelcast.stabilizer.tests.concurrent.atomiclong;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.performance.OperationsPerSecond;
import com.hazelcast.stabilizer.performance.Performance;
import com.hazelcast.stabilizer.tests.AbstractTest;
import com.hazelcast.stabilizer.tests.TestFailureException;
import com.hazelcast.stabilizer.tests.TestRunner;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class AtomicLongTest extends AbstractTest {

    private final static ILogger log = Logger.getLogger(AtomicLongTest.class);

    private IAtomicLong totalCounter;
    private IAtomicLong[] counters;
    private AtomicLong operations = new AtomicLong();

    //properties
    public int countersLength = 1000;
    public int threadCount = 1;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    public String basename = "atomiclong";

    @Override
    public void localSetup() throws Exception {
        log.info("countersLength:" + countersLength + " threadCount:" + threadCount);

        HazelcastInstance targetInstance = getTargetInstance();

        totalCounter = targetInstance.getAtomicLong(getTestId() + ":TotalCounter");
        counters = new IAtomicLong[countersLength];
        for (int k = 0; k < counters.length; k++) {
            counters[k] = targetInstance.getAtomicLong(basename + "-" + getTestId() + "r-" + k);
        }
    }

    @Override
    public void createTestThreads() {
        for (int k = 0; k < threadCount; k++) {
            spawn(new Worker());
        }
    }

    @Override
    public void globalVerify() {
        long expectedCount = totalCounter.get();
        long count = 0;
        for (IAtomicLong counter : counters) {
            count += counter.get();
        }

        if (expectedCount != count) {
            throw new TestFailureException("Expected count: " + expectedCount + " but found count was: " + count);
        }
    }

    @Override
    public void globalTearDown() throws Exception {
        for (IAtomicLong counter : counters) {
            counter.destroy();
        }
        totalCounter.destroy();
    }

    @Override
    public Performance calcPerformance() {
        OperationsPerSecond performance = new OperationsPerSecond();
        performance.setStartMs(getStartTimeMs());
        performance.setEndMs(getCurrentTimeMs());
        performance.setOperations(operations.get());
        return performance;
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            while (!stopped()) {
                IAtomicLong counter = getRandomCounter();
                counter.incrementAndGet();

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
                iteration++;
            }

            totalCounter.addAndGet(iteration);
        }

        private IAtomicLong getRandomCounter() {
            int index = random.nextInt(counters.length);
            return counters[index];
        }
    }

    public static void main(String[] args) throws Exception {
        AtomicLongTest test = new AtomicLongTest();
        new TestRunner().run(test, 60);
    }
}

