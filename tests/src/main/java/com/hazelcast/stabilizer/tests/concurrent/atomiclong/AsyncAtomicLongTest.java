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

import com.hazelcast.core.AsyncAtomicLong;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.test.annotations.Performance;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Teardown;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.tests.helpers.KeyLocality;
import com.hazelcast.stabilizer.tests.helpers.KeyUtils;
import com.hazelcast.stabilizer.test.utils.AssertTask;
import com.hazelcast.stabilizer.test.utils.ExceptionReporter;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;
import com.hazelcast.stabilizer.worker.Metronome;
import com.hazelcast.stabilizer.worker.OperationSelector;
import com.hazelcast.stabilizer.worker.SimpleMetronome;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.test.utils.TestUtils.assertTrueEventually;
import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static org.junit.Assert.assertEquals;

public class AsyncAtomicLongTest {

    private static final String KEY_PREFIX = "AsyncAtomicLongTest";

    private final static ILogger log = Logger.getLogger(AsyncAtomicLongTest.class);

    private enum Operation {
        PUT,
        GET
    }

    // Properties
    public int countersLength = 1000;
    public int threadCount = 10;
    public int logFrequency = 10000;
    public String basename = "atomicLong";
    public KeyLocality keyLocality = KeyLocality.Random;
    public double writeProb = 1.0;
    public int assertEventuallySeconds = 300;
    public int batchSize = -1;
    public int metronomeIntervalMillis = 50;

    private IAtomicLong totalCounter;
    private AsyncAtomicLong[] counters;
    private AtomicLong operations = new AtomicLong();
    private TestContext context;
    private HazelcastInstance targetInstance;
    private OperationSelector<Operation> selector = new OperationSelector<Operation>();

    @Setup
    public void setup(TestContext context) throws Exception {
        this.context = context;

        targetInstance = context.getTargetInstance();

        totalCounter = targetInstance.getAtomicLong("TotalCounter:" + context.getTestId());
        counters = new AsyncAtomicLong[countersLength];
        for (int i = 0; i < counters.length; i++) {
            String key = KEY_PREFIX + KeyUtils.generateStringKey(8, keyLocality, targetInstance);
            counters[i] = (AsyncAtomicLong) targetInstance.getAtomicLong(key);
        }

        selector
                .addOperation(Operation.PUT, writeProb)
                .addOperationRemainingProbability(Operation.GET);
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
        for (int i = 0; i < threadCount; i++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Verify
    public void verify() {
        final String serviceName = totalCounter.getServiceName();
        final long expected = totalCounter.get();

        // since the operations are asynchronous, we have no idea when they complete
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                // hack to prevent overloading the system with get calls, else it is done many times a second
                Utils.sleepSeconds(10);

                long actual = 0;
                for (DistributedObject distributedObject : targetInstance.getDistributedObjects()) {
                    String key = distributedObject.getName();
                    if (serviceName.equals(distributedObject.getServiceName()) && key.startsWith(KEY_PREFIX)) {
                        actual += targetInstance.getAtomicLong(key).get();
                    }
                }

                assertEquals(expected, actual);
            }
        }, assertEventuallySeconds);
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private class Worker implements Runnable, ExecutionCallback<Long> {
        private final Random random = new Random();
        private Metronome metronome = SimpleMetronome.withFixedIntervalMs(metronomeIntervalMillis);

        @Override
        public void run() {
            long iteration = 0;
            long increments = 0;

            List<ICompletableFuture> batch = new LinkedList<ICompletableFuture>();
            while (!context.isStopped()) {
                AsyncAtomicLong counter = getRandomCounter();

                ICompletableFuture<Long> future;
                switch (selector.select()) {
                    case PUT:
                        increments++;
                        future = counter.asyncIncrementAndGet();
                        break;
                    case GET:
                        future = counter.asyncGet();
                        break;
                    default:
                        throw new UnsupportedOperationException();
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

                metronome.waitForNext();
            }
            totalCounter.addAndGet(increments);
        }

        @Override
        public void onResponse(Long response) {
            operations.addAndGet(1);
        }

        @Override
        public void onFailure(Throwable t) {
            ExceptionReporter.report(context.getTestId(), t);
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

