/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.icache;

import com.hazelcast.cache.ICache;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.io.Serializable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.getCache;
import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.sleepDurationTwice;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * In this tests we a putting and getting to/from a cache using an expiry policy.
 * The expiryDuration can be configured.
 * We verify that the cache is empty and items have expired.
 */
public class ExpiryTest {

    private enum Operation {
        PUT,
        PUT_ASYNC,
        GET,
        GET_ASYNC
    }

    private static final ILogger LOGGER = Logger.getLogger(ExpiryTest.class);

    public String basename = ExpiryTest.class.getSimpleName();
    public int expiryDuration = 500;
    public int keyCount = 1000;

    public double putProb = 0.4;
    public double putAsyncProb = 0.3;
    public double getProb = 0.2;
    public double getAsyncProb = 0.1;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();
    private final ExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, expiryDuration));

    private ICache<Integer, Long> cache;
    private IList<Counter> results;

    @Setup
    public void setup(TestContext testContext) {
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
        cache = getCache(hazelcastInstance, basename);
        results = hazelcastInstance.getList(basename);

        operationSelectorBuilder
                .addOperation(Operation.PUT, putProb)
                .addOperation(Operation.PUT_ASYNC, putAsyncProb)
                .addOperation(Operation.GET, getProb)
                .addOperation(Operation.GET_ASYNC, getAsyncProb);
    }

    @Verify
    public void globalVerify() {
        Counter totalCounter = new Counter();
        for (Counter counter : results) {
            totalCounter.add(counter);
        }
        LOGGER.info(basename + " " + totalCounter + " from " + results.size() + " worker Threads");

        for (int i = 0; i < keyCount; i++) {
            assertFalse(basename + " ICache should not contain key " + i, cache.containsKey(i));
        }
        assertFalse(basename + " ICache iterator should not have elements", cache.iterator().hasNext());
        assertEquals(basename + " ICache size should be 0", 0, cache.size());
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        private final Counter counter = new Counter();

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        public void timeStep(Operation operation) throws Exception {
            int key = randomInt(keyCount);

            switch (operation) {
                case PUT:
                    cache.put(key, getRandom().nextLong(), expiryPolicy);
                    counter.putExpiry++;
                    break;
                case PUT_ASYNC:
                    cache.putAsync(key, getRandom().nextLong(), expiryPolicy);
                    counter.putAsyncExpiry++;
                    break;
                case GET:
                    cache.get(key, expiryPolicy);
                    counter.getExpiry++;
                    break;
                case GET_ASYNC:
                    Future<Long> future = cache.getAsync(key, expiryPolicy);
                    future.get();
                    counter.getAsyncExpiry++;
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown operation " + operation);
            }
        }

        @Override
        protected void afterRun() {
            results.add(counter);

            // sleep to give time for expiration
            sleepDurationTwice(LOGGER, expiryPolicy.getExpiryForCreation());
        }
    }

    private static class Counter implements Serializable {

        private long putExpiry;
        private long putAsyncExpiry;
        private long getExpiry;
        private long getAsyncExpiry;

        public void add(Counter counter) {
            putExpiry += counter.putExpiry;
            putAsyncExpiry += counter.putAsyncExpiry;
            getExpiry += counter.getExpiry;
            getAsyncExpiry += counter.getAsyncExpiry;
        }

        public String toString() {
            return "Counter{"
                    + "putExpiry=" + putExpiry
                    + ", putAsyncExpiry=" + putAsyncExpiry
                    + ", getExpiry=" + getExpiry
                    + ", getAsyncExpiry=" + getAsyncExpiry
                    + '}';
        }
    }
}
