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

import com.hazelcast.core.AsyncAtomicLong;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.helpers.KeyUtils;
import com.hazelcast.simulator.test.utils.AssertTask;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.test.utils.ThreadSpawner;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.test.utils.TestUtils.assertTrueEventually;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static org.junit.Assert.assertEquals;

public class AsyncAtomicLongTest {

    private static final ILogger log = Logger.getLogger(AsyncAtomicLongTest.class);

    //props
    public int countersLength = 1000;
    public int threadCount = 10;
    public int logFrequency = 10000;
    public String basename = "atomiclong";
    public KeyLocality keyLocality = KeyLocality.Random;
    public int writePercentage = 100;
    public int assertEventuallySeconds = 300;
    public int batchSize = -1;

    private IAtomicLong totalCounter;
    private AsyncAtomicLong[] counters;
    private AtomicLong operations = new AtomicLong();
    private TestContext context;
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

        totalCounter = targetInstance.getAtomicLong(context.getTestId() + ":TotalCounter");
        counters = new AsyncAtomicLong[countersLength];
        for (int k = 0; k < counters.length; k++) {
            String key = KeyUtils.generateStringKey(8, keyLocality, targetInstance);
            counters[k] = (AsyncAtomicLong) targetInstance.getAtomicLong(key);
        }
    }

    @Teardown
    public void teardown() throws Exception {
        for (IAtomicLong counter : counters) {
            counter.destroy();
        }
        totalCounter.destroy();
        log.info(getOperationCountInformation(targetInstance));
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(context.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Verify
    public void verify() {
        final long expected = totalCounter.get();

        // since the operations are asynchronous, we have no idea when they complete
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                // hack to prevent overloading the system with get calls. Else it is done many times a second
                sleepSeconds(10);

                long actual = 0;
                for (IAtomicLong counter : counters) {
                    actual += counter.get();
                }
                assertEquals(expected, actual);
            }
        }, assertEventuallySeconds);
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private class Worker implements Runnable, ExecutionCallback {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            long increments = 0;

            List<ICompletableFuture> batch = new LinkedList<ICompletableFuture>();
            while (!context.isStopped()) {
                AsyncAtomicLong counter = getRandomCounter();
                ICompletableFuture<Long> future;
                if (shouldWrite(iteration)) {
                    increments++;
                    future = counter.asyncIncrementAndGet();
                } else {
                    future = counter.asyncGet();
                }

                future.andThen(this);

                if (batchSize > 0) {
                    batch.add(future);

                    if (batch.size() == batchSize) {
                        for (ICompletableFuture f : batch) {
                            try {
                                f.get();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            } catch (ExecutionException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                }

                iteration++;
                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }
            }
            totalCounter.addAndGet(increments);
        }

        @Override
        public void onResponse(Object response) {
            operations.addAndGet(1);
        }

        @Override
        public void onFailure(Throwable t) {
            ExceptionReporter.report(context.getTestId(), t);
        }

        private boolean shouldWrite(long iteration) {
            if (writePercentage == 0) {
                return false;
            } else if (writePercentage == 100) {
                return true;
            } else {
                return random.nextInt(100) <= writePercentage;
            }
        }

        private AsyncAtomicLong getRandomCounter() {
            int index = random.nextInt(counters.length);
            return counters[index];
        }
    }

    public static void main(String[] args) throws Throwable {
        AsyncAtomicLongTest test = new AsyncAtomicLongTest();
        new TestRunner<AsyncAtomicLongTest>(test).withDuration(10).run();
    }
}

