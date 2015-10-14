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
package com.hazelcast.simulator.tests.concurrent.atomiclong;

import com.hazelcast.core.AsyncAtomicLong;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractAsyncWorker;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isClient;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKey;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static com.hazelcast.simulator.worker.metronome.SimpleMetronome.withFixedIntervalMs;
import static org.junit.Assert.assertEquals;

public class AsyncAtomicLongTest {

    private static final ILogger LOGGER = Logger.getLogger(AsyncAtomicLongTest.class);

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public String basename = AsyncAtomicLongTest.class.getSimpleName();
    public KeyLocality keyLocality = KeyLocality.RANDOM;
    public int countersLength = 1000;
    public int metronomeIntervalMs = (int) TimeUnit.SECONDS.toMillis(1);
    public int assertEventuallySeconds = 300;
    public int batchSize = -1;

    public double writeProb = 1.0;

    private final OperationSelectorBuilder<Operation> builder = new OperationSelectorBuilder<Operation>();

    private IAtomicLong totalCounter;
    private AsyncAtomicLong[] counters;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext context) throws Exception {
        targetInstance = context.getTargetInstance();

        totalCounter = targetInstance.getAtomicLong("TotalCounter:" + context.getTestId());
        if (isMemberNode(targetInstance)) {
            counters = new AsyncAtomicLong[countersLength];
            for (int i = 0; i < counters.length; i++) {
                String key = basename + generateStringKey(8, keyLocality, targetInstance);
                counters[i] = (AsyncAtomicLong) targetInstance.getAtomicLong(key);
            }
        }

        builder.addOperation(Operation.PUT, writeProb)
                .addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void teardown() throws Exception {
        if (isMemberNode(targetInstance)) {
            for (IAtomicLong counter : counters) {
                counter.destroy();
            }
        }
        totalCounter.destroy();
        LOGGER.info(getOperationCountInformation(targetInstance));
    }

    @Verify
    public void verify() {
        if (isClient(targetInstance)) {
            return;
        }

        final String serviceName = totalCounter.getServiceName();
        final long expected = totalCounter.get();

        // since the operations are asynchronous, we have no idea when they complete
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                // hack to prevent overloading the system with get calls, else it is done many times a second
                sleepSeconds(10);

                long actual = 0;
                for (DistributedObject distributedObject : targetInstance.getDistributedObjects()) {
                    String key = distributedObject.getName();
                    if (serviceName.equals(distributedObject.getServiceName()) && key.startsWith(basename)) {
                        actual += targetInstance.getAtomicLong(key).get();
                    }
                }

                assertEquals(expected, actual);
            }
        }, assertEventuallySeconds);
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractAsyncWorker<Operation, Long> {

        private final List<ICompletableFuture> batch = new LinkedList<ICompletableFuture>();
        private final Metronome metronome = withFixedIntervalMs(metronomeIntervalMs);

        private long increments;

        public Worker() {
            super(builder);
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            if (isClient(targetInstance)) {
                return;
            }

            AsyncAtomicLong counter = getRandomCounter();

            ICompletableFuture<Long> future;
            switch (operation) {
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
                    for (ICompletableFuture batchFuture : batch) {
                        try {
                            batchFuture.get();
                        } catch (InterruptedException e) {
                            throw new TestException(e);
                        } catch (ExecutionException e) {
                            throw new TestException(e);
                        }
                    }
                    batch.clear();
                }
            }

            metronome.waitForNext();
        }

        @Override
        protected void afterRun() {
            totalCounter.addAndGet(increments);
        }

        private AsyncAtomicLong getRandomCounter() {
            int index = randomInt(counters.length);
            return counters[index];
        }
    }

    public static void main(String[] args) throws Exception {
        AsyncAtomicLongTest test = new AsyncAtomicLongTest();
        new TestRunner<AsyncAtomicLongTest>(test).withDuration(10).run();
    }
}
