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
import com.hazelcast.core.IList;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

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
 * In this tests we are putting and getting to/from a cache using an expiry policy.
 * The expiryDuration can be configured.
 * We verify that the cache is empty and items have expired.
 */
public class TTLICacheTest extends HazelcastTest {

    public int expiryDuration = 500;
    public int keyCount = 1000;

    private final ExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, expiryDuration));
    private ICache<Integer, Long> cache;
    private IList<Counter> results;

    @Setup
    public void setup() {
        cache = getCache(targetInstance, name);
        results = targetInstance.getList(name);
    }

    @TimeStep(prob = 0.4)
    public void put(ThreadState state) {
        int key = state.randomInt(keyCount);
        cache.put(key, state.randomLong(), expiryPolicy);
        state.counter.putExpiry++;
    }

    @TimeStep(prob = 0.3)
    public void putAsync(ThreadState state) {
        int key = state.randomInt(keyCount);
        cache.putAsync(key, state.randomLong(), expiryPolicy);
        state.counter.putAsyncExpiry++;
    }

    @TimeStep(prob = 0.2)
    public void get(ThreadState state) {
        int key = state.randomInt(keyCount);
        cache.get(key, expiryPolicy);
        state.counter.getExpiry++;
    }

    @TimeStep(prob = 0.1)
    public void getAsync(ThreadState state) throws Exception {
        int key = state.randomInt(keyCount);
        Future<Long> future = cache.getAsync(key, expiryPolicy);
        future.get();
        state.counter.getAsyncExpiry++;
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        results.add(state.counter);

        // sleep to give time for expiration
        sleepDurationTwice(logger, expiryPolicy.getExpiryForCreation());
    }

    public class ThreadState extends BaseThreadState {

        private final Counter counter = new Counter();
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

    @Verify
    public void globalVerify() {
        Counter totalCounter = new Counter();
        for (Counter counter : results) {
            totalCounter.add(counter);
        }
        logger.info(name + " " + totalCounter + " from " + results.size() + " worker Threads");

        for (int i = 0; i < keyCount; i++) {
            assertFalse(name + " ICache should not contain key " + i, cache.containsKey(i));
        }
        assertFalse(name + " ICache iterator should not have elements", cache.iterator().hasNext());
        assertEquals(name + " ICache size should be 0", 0, cache.size());
    }
}
