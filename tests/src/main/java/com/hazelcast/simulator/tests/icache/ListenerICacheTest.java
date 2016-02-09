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
import com.hazelcast.simulator.tests.icache.helpers.CacheUtils;
import com.hazelcast.simulator.tests.icache.helpers.ICacheEntryEventFilter;
import com.hazelcast.simulator.tests.icache.helpers.ICacheEntryListener;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.io.Serializable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static org.junit.Assert.assertEquals;

/**
 * In this test we add listeners to a cache and record the number of events the listeners receive.
 * We compare those to the number of events we have generated using different cache operations.
 * We verify that no unexpected events have been received.
 */
public class ListenerICacheTest {

    private static final int PAUSE_FOR_LAST_EVENTS_SECONDS = 10;

    private static final ILogger LOGGER = Logger.getLogger(ListenerICacheTest.class);

    private enum Operation {
        PUT,
        PUT_EXPIRY,
        PUT_EXPIRY_ASYNC,
        GET_EXPIRY,
        GET_EXPIRY_ASYNC,
        REMOVE,
        REPLACE
    }

    public String basename = ListenerICacheTest.class.getSimpleName();
    public int keyCount = 1000;
    public int maxExpiryDurationMs = 500;
    public boolean syncEvents = true;

    public double put = 0.8;
    public double putExpiry = 0.0;
    public double putAsyncExpiry = 0.0;
    public double getExpiry = 0.0;
    public double getAsyncExpiry = 0.0;
    public double remove = 0.1;
    public double replace = 0.1;

    private final OperationSelectorBuilder<Operation> builder = new OperationSelectorBuilder<Operation>();

    private IList<Counter> results;
    private IList<ICacheEntryListener> listeners;

    private ICache<Integer, Long> cache;
    private ICacheEntryListener<Integer, Long> listener;
    private ICacheEntryEventFilter<Integer, Long> filter;

    @Setup
    public void setup(TestContext testContext) {
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
        results = hazelcastInstance.getList(basename);
        listeners = hazelcastInstance.getList(basename + "listeners");

        cache = CacheUtils.getCache(hazelcastInstance, basename);
        listener = new ICacheEntryListener<Integer, Long>();
        filter = new ICacheEntryEventFilter<Integer, Long>();

        CacheEntryListenerConfiguration<Integer, Long> config = new MutableCacheEntryListenerConfiguration<Integer, Long>(
                FactoryBuilder.factoryOf(listener),
                FactoryBuilder.factoryOf(filter),
                false, syncEvents);
        cache.registerCacheEntryListener(config);

        builder.addOperation(Operation.PUT, put)
                .addOperation(Operation.PUT_EXPIRY, putExpiry)
                .addOperation(Operation.PUT_EXPIRY_ASYNC, putAsyncExpiry)
                .addOperation(Operation.GET_EXPIRY, getExpiry)
                .addOperation(Operation.GET_EXPIRY_ASYNC, getAsyncExpiry)
                .addOperation(Operation.REMOVE, remove)
                .addOperation(Operation.REPLACE, replace);
    }

    @Verify(global = false)
    public void localVerify() {
        LOGGER.info(basename + " Listener " + listener);
        LOGGER.info(basename + " Filter " + filter);
    }

    @Verify
    public void globalVerify() {
        Counter totalCounter = new Counter();
        for (Counter counter : results) {
            totalCounter.add(counter);
        }
        LOGGER.info(basename + " " + totalCounter + " from " + results.size() + " Worker threads");

        ICacheEntryListener totalEvents = new ICacheEntryListener();
        for (ICacheEntryListener entryListener : listeners) {
            totalEvents.add(entryListener);
        }
        LOGGER.info(basename + " totalEvents: " + totalEvents);
        assertEquals(basename + " unexpected events found", 0, totalEvents.getUnexpected());
    }

    @RunWithWorker
    public Worker run() {
        return new Worker();
    }

    private final class Worker extends AbstractWorker<Operation> {

        private final Counter counter = new Counter();

        private Worker() {
            super(builder);
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            int expiryDuration = randomInt(maxExpiryDurationMs);
            ExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, expiryDuration));

            int key = randomInt(keyCount);

            switch (operation) {
                case PUT:
                    cache.put(key, getRandom().nextLong());
                    counter.put++;
                    break;

                case PUT_EXPIRY:
                    cache.put(key, getRandom().nextLong(), expiryPolicy);
                    counter.putExpiry++;
                    break;

                case PUT_EXPIRY_ASYNC:
                    cache.putAsync(key, getRandom().nextLong(), expiryPolicy);
                    counter.putAsyncExpiry++;
                    break;

                case GET_EXPIRY:
                    cache.get(key, expiryPolicy);
                    counter.getExpiry++;
                    break;

                case GET_EXPIRY_ASYNC:
                    Future<Long> future = cache.getAsync(key, expiryPolicy);
                    future.get();
                    counter.getAsyncExpiry++;
                    break;

                case REMOVE:
                    if (cache.remove(key)) {
                        counter.remove++;
                    }
                    break;

                case REPLACE:
                    if (cache.replace(key, getRandom().nextLong())) {
                        counter.replace++;
                    }
                    break;

                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        protected void afterRun() {
            results.add(counter);
        }

        @Override
        public void afterCompletion() {
            listeners.add(listener);

            sleepSeconds(PAUSE_FOR_LAST_EVENTS_SECONDS);
        }
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
