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
package com.hazelcast.simulator.tests.concurrent.atomiclong;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.helpers.KeyUtils;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static org.junit.Assert.assertEquals;

public class AtomicLongOldTest {

    private static final ILogger log = Logger.getLogger(AtomicLongOldTest.class);

    //props
    public int countersLength = 1000;
    public int threadCount = 10;
  //  public int logFrequency = 10000;
    public int performanceUpdateFrequency = 1000;
    public String basename = "atomiclong";
    public KeyLocality keyLocality = KeyLocality.RANDOM;
    public int writePercentage = 100;
    public int warmupIterations = 100;

    private IAtomicLong totalCounter;
    private IAtomicLong[] counters;
    private TestContext context;
    private HazelcastInstance targetInstance;
    private AtomicLong ops2 = new AtomicLong();
    private long startMs;
    private long endMs;

    @InjectProbe(useForThroughput = true)
    private Probe probe;

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

        totalCounter = targetInstance.getAtomicLong(context.getTestId() + ":TotalCounter");
        counters = new IAtomicLong[countersLength];
        for (int k = 0; k < counters.length; k++) {
            String key = KeyUtils.generateStringKey(8, keyLocality, targetInstance);
            counters[k] = targetInstance.getAtomicLong(key);
        }
    }

    @Warmup
    public void warmup() {
        for (int k = 0; k < warmupIterations; k++) {
            for (IAtomicLong counter : counters) {
                counter.get();
            }
        }
    }

    @Teardown
    public void teardown() throws Exception {
        for (IAtomicLong counter : counters) {
            counter.destroy();
        }
        log.warning("---Operations:"+ops2);
        log.warning("---Throughput:"+((ops2.get()*1000d)/(endMs-startMs))+" ops/second");


        totalCounter.destroy();
        log.info(getOperationCountInformation(targetInstance));
    }

    @Run
    public void run() {
        startMs = System.currentTimeMillis();
        ThreadSpawner spawner = new ThreadSpawner(context.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
        endMs = System.currentTimeMillis();
    }

    @Verify
    public void verify() {
        long expected = totalCounter.get();
        long actual = 0;
        for (IAtomicLong counter : counters) {
            actual += counter.get();
        }

        assertEquals(expected, actual);
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            long increments = 0;

            while (!context.isStopped()) {
                IAtomicLong counter = getRandomCounter();
                if (isWrite()) {
                    increments++;
                    counter.incrementAndGet();
                } else {
                    counter.get();
                }

                iteration++;
//                if (iteration % logFrequency == 0) {
//                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
//                }
                if (iteration % performanceUpdateFrequency == 0) {
                    probe.inc(performanceUpdateFrequency);
                }
            }
            probe.inc(iteration % performanceUpdateFrequency);
            totalCounter.addAndGet(increments);
        }

        private boolean isWrite() {
            if (writePercentage == 100) {
                return true;
            } else if (writePercentage == 0) {
                return false;
            } else {
                return random.nextInt(100) <= writePercentage;
            }
        }

        private IAtomicLong getRandomCounter() {
            int index = random.nextInt(counters.length);
            return counters[index];
        }
    }

    public static void main(String[] args) throws Throwable {
        AtomicLongOldTest test = new AtomicLongOldTest();
        new TestRunner<AtomicLongOldTest>(test).withDuration(10).run();
    }
}

