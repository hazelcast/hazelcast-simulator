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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.tests.helpers.KeyLocality;
import com.hazelcast.stabilizer.tests.helpers.KeyUtils;
import com.hazelcast.stabilizer.test.utils.AssertTask;
import com.hazelcast.stabilizer.tests.StabilizerAbstractTest;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.stabilizer.test.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;

public class AsyncAtomicLongTest extends StabilizerAbstractTest {

    private static final String KEY_PREFIX = "AsyncAtomicLongTest";

    // Properties
    public int countersLength = 1000;
    public String basename = "atomicLong";
    public KeyLocality keyLocality = KeyLocality.Random;
    public double writeProb = 1.0;
    public int assertEventuallySeconds = 300;
    public int batchSize = -1;

    private AsyncAtomicLong[] counters;
    private String serviceName;

    @Override
    public void afterSetup(HazelcastInstance hazelcastInstance) throws Exception {
        counters = new AsyncAtomicLong[countersLength];
        for (int i = 0; i < counters.length; i++) {
            String key = KEY_PREFIX + KeyUtils.generateStringKey(8, keyLocality, hazelcastInstance);
            counters[i] = (AsyncAtomicLong) hazelcastInstance.getAtomicLong(key);
        }
        serviceName = counters[0].getServiceName();

        addOperation(BaseOperation.PUT, writeProb);
        addOperationRemainingProbability(BaseOperation.GET);
    }

    @Override
    public void beforeTeardown() throws Exception {
        for (IAtomicLong counter : counters) {
            counter.destroy();
        }
    }

    @Override
    public void doVerify(final HazelcastInstance hazelcastInstance, final long verifyCounter) {
        // since the operations are asynchronous, we have no idea when they complete
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                // hack to prevent overloading the system with get calls. Else it is done many times a second
                Utils.sleepSeconds(10);

                long actual = 0;
                for (DistributedObject distributedObject : hazelcastInstance.getDistributedObjects()) {
                    String key = distributedObject.getName();
                    if ((distributedObject.getServiceName().equals(serviceName) && key.startsWith(KEY_PREFIX))) {
                        actual += hazelcastInstance.getAtomicLong(key).get();
                    }
                }
                assertEquals(verifyCounter, actual);
            }
        }, assertEventuallySeconds);
    }

    @Override
    public BaseWorker createWorker() {
        return new Worker();
    }

    private class Worker extends AsyncBaseWorker<Long> {
        private final List<ICompletableFuture<Long>> batch = new LinkedList<ICompletableFuture<Long>>();

        @Override
        protected void doRun(BaseOperation baseOperation) {
            AsyncAtomicLong counter = getRandomCounter();

            ICompletableFuture<Long> future;
            switch (baseOperation) {
                case PUT:
                    incrementVerifyCounter();
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
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }

        @Override
        protected void onAsyncResponse(Long type) {
        }

        private AsyncAtomicLong getRandomCounter() {
            return counters[getRandomInt(counters.length)];
        }
    }

    public static void main(String[] args) throws Throwable {
        AsyncAtomicLongTest test = new AsyncAtomicLongTest();
        new TestRunner<AsyncAtomicLongTest>(test).withDuration(10).run();
    }
}

