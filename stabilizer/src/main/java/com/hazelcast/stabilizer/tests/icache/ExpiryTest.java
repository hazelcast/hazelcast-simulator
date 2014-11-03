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
import javax.cache.CacheManager;;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * In This tests we a putting and getting to/from a cache using an Expiry Policy
 * the expiryDuration can be configured
 * we Verify that the cache is empty and items have expired
 * */
public class ExpiryTest {

    private final static ILogger log = Logger.getLogger(ExpiryTest.class);

    public int threadCount = 3;
    public int expiryDuration = 500;
    public int keyCount = 1000;

    public double putExpiry = 0.4;
    public double putAsyncExpiry = 0.3;
    public double getExpiry = 0.2;
    public double getAsyncExpiry = 0.1;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private CacheManager cacheManager;
    private String basename;

    private ExpiryPolicy expiryPolicy;
    private CacheConfig<Integer, Long> config = new CacheConfig();

    @Setup
    public void setup(TestContext textConTx) {
        testContext = textConTx;
        targetInstance = testContext.getTargetInstance();
        basename = testContext.getTestId();

        if (TestUtils.isMemberNode(targetInstance)) {
            HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
            cacheManager = new HazelcastServerCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        } else {
            HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
            cacheManager = new HazelcastClientCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        }
        expiryPolicy = new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, expiryDuration));

        config.setName(basename);
    }

    @Warmup(global = true)
    public void warmup() {
        cacheManager.createCache(basename, config);
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        private Random random = new Random();
        private Counter counter = new Counter();
        private ICache<Integer, Long> cache = (ICache) cacheManager.getCache(basename);

        public void run() {
            while (!testContext.isStopped()) {
                int k = random.nextInt(keyCount);

                double chance = random.nextDouble();
                if ((chance -= putExpiry) < 0) {
                    cache.put(k, random.nextLong(), expiryPolicy);
                    counter.putExpiry++;

                } else if ((chance -= putAsyncExpiry) < 0) {
                    cache.putAsync(k, random.nextLong(), expiryPolicy);
                    counter.putAsyncExpiry++;

                } else if ((chance -= getExpiry) < 0) {
                    Long value = cache.get(k, expiryPolicy);
                    counter.getExpiry++;

                } else if ((chance -= getAsyncExpiry) < 0) {
                    Future<Long> f = cache.getAsync(k, expiryPolicy);
                    try {
                        f.get();
                        counter.getAsyncExpiry++;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                }
            }
            targetInstance.getList(basename).add(counter);
        }
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {

        IList<Counter> results = targetInstance.getList(basename);
        Counter total = new Counter();
        for (Counter i : results) {
            total.add(i);
        }
        log.info(basename + ": " + total + " from " + results.size() + " worker Threads");

        final ICache<Integer, Long> cache = (ICache) cacheManager.getCache(basename);

        for(int i=0; i<keyCount; i++){
            assertFalse(basename + ": cache should not contain any keys ", cache.containsKey(i) );
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
