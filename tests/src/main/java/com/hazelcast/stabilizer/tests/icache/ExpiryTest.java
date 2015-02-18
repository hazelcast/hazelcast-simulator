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
package com.hazelcast.stabilizer.tests.icache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Performance;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;
import com.hazelcast.stabilizer.worker.selector.OperationSelector;
import com.hazelcast.stabilizer.worker.selector.OperationSelectorBuilder;

import javax.cache.CacheManager;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.isMemberNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * In This tests we a putting and getting to/from a cache using an Expiry Policy
 * the expiryDuration can be configured
 * we Verify that the cache is empty and items have expired
 */
public class ExpiryTest {

    private enum Operation {
        PUT,
        PUT_ASYNC,
        GET,
        GET_ASYNC
    }

    private final static ILogger log = Logger.getLogger(ExpiryTest.class);

    public int threadCount = 3;
    public int expiryDuration = 500;
    public int keyCount = 1000;

    public double putProb = 0.4;
    public double putAsyncProb = 0.3;
    public double getProb = 0.2;
    public double getAsyncProb = 0.1;

    public int performanceUpdateFrequency = 10000;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();
    private AtomicLong operations = new AtomicLong();
    private CacheManager cacheManager;
    private String basename;

    private ExpiryPolicy expiryPolicy;
    private CacheConfig<Integer, Long> config = new CacheConfig<Integer, Long>();

    @Setup
    public void setup(TestContext textConTx) {
        testContext = textConTx;
        targetInstance = testContext.getTargetInstance();
        basename = testContext.getTestId();

        if (isMemberNode(targetInstance)) {
            HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
            cacheManager = new HazelcastServerCacheManager(hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(),
                    null);
        } else {
            HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
            cacheManager = new HazelcastClientCacheManager(hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(),
                    null);
        }
        expiryPolicy = new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, expiryDuration));

        config.setName(basename);

        operationSelectorBuilder.addOperation(Operation.PUT, putProb).addOperation(Operation.PUT_ASYNC, putAsyncProb)
                                .addOperation(Operation.GET, getProb).addOperation(Operation.GET_ASYNC, getAsyncProb);
    }

    @Warmup(global = true)
    public void warmup() {
        cacheManager.createCache(basename, config);
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int i = 0; i < threadCount; i++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        private final OperationSelector<Operation> selector = operationSelectorBuilder.build();
        private final Random random = new Random();
        private final Counter counter = new Counter();
        @SuppressWarnings("unchecked")
        private final ICache<Integer, Long> cache = (ICache) cacheManager.getCache(basename);

        public void run() {
            long iteration = 0;
            while (!testContext.isStopped()) {
                int key = random.nextInt(keyCount);

                Operation operation = selector.select();
                switch (operation) {
                    case PUT:
                        cache.put(key, random.nextLong(), expiryPolicy);
                        counter.putExpiry++;
                        break;
                    case PUT_ASYNC:
                        cache.putAsync(key, random.nextLong(), expiryPolicy);
                        counter.putAsyncExpiry++;
                        break;
                    case GET:
                        cache.get(key, expiryPolicy);
                        counter.getExpiry++;
                        break;
                    case GET_ASYNC:
                        Future<Long> f = cache.getAsync(key, expiryPolicy);
                        try {
                            f.get();
                            counter.getAsyncExpiry++;
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                        break;
                    default:
                        throw new UnsupportedOperationException("Unknown operation" + operation);
                }
                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
                iteration++;
            }
            operations.addAndGet(iteration % performanceUpdateFrequency);
            targetInstance.getList(basename).add(counter);
        }
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {

        IList<Counter> results = targetInstance.getList(basename);
        Counter totalCounter = new Counter();
        for (Counter counter : results) {
            totalCounter.add(counter);
        }
        log.info(basename + ": " + totalCounter + " from " + results.size() + " worker Threads");

        @SuppressWarnings("unchecked") final ICache<Integer, Long> cache = (ICache) cacheManager.getCache(basename);

        for (int i = 0; i < keyCount; i++) {
            assertFalse(basename + ": cache should not contain any keys ", cache.containsKey(i));
        }

        assertFalse(basename + ": iterator should not have elements ", cache.iterator().hasNext());
        assertEquals(basename + ": cache size not 0", 0, cache.size());
    }

    private static class Counter implements Serializable {
        public long putExpiry;
        public long putAsyncExpiry;
        public long getExpiry;
        public long getAsyncExpiry;

        public void add(Counter c) {
            putExpiry += c.putExpiry;
            putAsyncExpiry += c.putAsyncExpiry;
            getExpiry += c.getExpiry;
            getAsyncExpiry += c.getAsyncExpiry;
        }

        public String toString() {
            return "Counter{" +
                    "putExpiry=" + putExpiry +
                    ", putAsyncExpiry=" + putAsyncExpiry +
                    ", getExpiry=" + getExpiry +
                    ", getAsyncExpiry=" + getAsyncExpiry +
                    '}';
        }
    }
}
