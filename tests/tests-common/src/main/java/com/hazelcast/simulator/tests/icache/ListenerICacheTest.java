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
import com.hazelcast.simulator.test.BaseThreadContext;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.AbstractTest;
import com.hazelcast.simulator.tests.icache.helpers.CacheUtils;
import com.hazelcast.simulator.tests.icache.helpers.ICacheEntryEventFilter;
import com.hazelcast.simulator.tests.icache.helpers.ICacheEntryListener;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * In this test we add listeners to a cache and record the number of events the listeners receive.
 * We compare those to the number of events we have generated using different cache operations.
 * We verify that no unexpected events have been received.
 */
public class ListenerICacheTest extends AbstractTest {

    private static final int PAUSE_FOR_LAST_EVENTS_SECONDS = 10;

    public int keyCount = 1000;
    public int maxExpiryDurationMs = 500;
    public boolean syncEvents = true;

    private IList<Counter> results;
    private IList<ICacheEntryListener> listeners;

    private ICache<Integer, Long> cache;
    private ICacheEntryListener<Integer, Long> listener;
    private ICacheEntryEventFilter<Integer, Long> filter;

    @Setup
    public void setup() {
        results = targetInstance.getList(name);
        listeners = targetInstance.getList(name + "listeners");

        cache = CacheUtils.getCache(targetInstance, name);
        listener = new ICacheEntryListener<Integer, Long>();
        filter = new ICacheEntryEventFilter<Integer, Long>();

        CacheEntryListenerConfiguration<Integer, Long> config = new MutableCacheEntryListenerConfiguration<Integer, Long>(
                FactoryBuilder.factoryOf(listener),
                FactoryBuilder.factoryOf(filter),
                false, syncEvents);
        cache.registerCacheEntryListener(config);
    }

    @TimeStep(prob = 0.8)
    public void put(ThreadContext context) {
        int key = context.randomInt(keyCount);
        cache.put(key, context.randomLong());
        context.counter.put++;
    }

    @TimeStep(prob = 0)
    public void putExpiry(ThreadContext context) {
        int key = context.randomInt(keyCount);
        int expiryDuration = context.randomInt(maxExpiryDurationMs);
        cache.put(key, context.randomLong(), newExpiryPolicy(expiryDuration));
        context.counter.putExpiry++;
    }

    @TimeStep(prob = 0)
    public void putExpiryAsync(ThreadContext context) {
        int key = context.randomInt(keyCount);
        int expiryDuration = context.randomInt(maxExpiryDurationMs);
        cache.putAsync(key, context.randomLong(), newExpiryPolicy(expiryDuration));
        context.counter.putAsyncExpiry++;
    }

    @TimeStep(prob = 0)
    public void getExpiry(ThreadContext context) {
        int key = context.randomInt(keyCount);
        int expiryDuration = context.randomInt(maxExpiryDurationMs);
        cache.get(key, newExpiryPolicy(expiryDuration));
        context.counter.getExpiry++;
    }

    @TimeStep(prob = 0)
    public void getExpiryAsync(ThreadContext context) throws ExecutionException, InterruptedException {
        int key = context.randomInt(keyCount);
        int expiryDuration = context.randomInt(maxExpiryDurationMs);
        Future<Long> future = cache.getAsync(key, newExpiryPolicy(expiryDuration));
        future.get();
        context.counter.getAsyncExpiry++;
    }

    @TimeStep(prob = 0.1)
    public void remove(ThreadContext context) {
        int key = context.randomInt(keyCount);
        if (cache.remove(key)) {
            context.counter.remove++;
        }
    }

    @TimeStep(prob = 0.1)
    public void replace(ThreadContext context) {
        int key = context.randomInt(keyCount);
        if (cache.replace(key, context.randomLong())) {
            context.counter.replace++;
        }
    }

    public final class ThreadContext extends BaseThreadContext {

        private final Counter counter = new Counter();

        //todo:
//   @Override
//        public void afterCompletion() {
//            listeners.add(listener);
//
//            sleepSeconds(PAUSE_FOR_LAST_EVENTS_SECONDS);
//        }
    }

    @AfterRun
    public void afterRun(ThreadContext context) {
        results.add(context.counter);
    }

    @Verify(global = false)
    public void localVerify() {
        logger.info(name + " Listener " + listener);
        logger.info(name + " Filter " + filter);
    }

    @Verify
    public void globalVerify() {
        Counter totalCounter = new Counter();
        for (Counter counter : results) {
            totalCounter.add(counter);
        }
        logger.info(name + " " + totalCounter + " from " + results.size() + " Worker threads");

        ICacheEntryListener totalEvents = new ICacheEntryListener();
        for (ICacheEntryListener entryListener : listeners) {
            totalEvents.add(entryListener);
        }
        logger.info(name + " totalEvents: " + totalEvents);
        assertEquals(name + " unexpected events found", 0, totalEvents.getUnexpected());
    }

    private ExpiryPolicy newExpiryPolicy(int expiryDuration) {
        return new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, expiryDuration));
    }

    private static class Counter implements Serializable {

        public long put;
        public long putExpiry;
        public long putAsyncExpiry;
        public long getExpiry;
        public long getAsyncExpiry;
        public long remove;
        public long replace;

        public void add(Counter counter) {
            put += counter.put;
            putExpiry += counter.putExpiry;
            putAsyncExpiry += counter.putAsyncExpiry;
            getExpiry += counter.getExpiry;
            getAsyncExpiry += counter.getAsyncExpiry;
            remove += counter.remove;
            replace += counter.replace;
        }

        public String toString() {
            return "Counter{"
                    + "put=" + put
                    + ", putExpiry=" + putExpiry
                    + ", putAsyncExpiry=" + putAsyncExpiry
                    + ", getExpiry=" + getExpiry
                    + ", getAsyncExpiry=" + getAsyncExpiry
                    + ", remove=" + remove
                    + ", replace=" + replace
                    + '}';
        }
    }
}
