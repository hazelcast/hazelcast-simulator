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

import javax.cache.CacheManager;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.test.annotations.Performance;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;

import javax.cache.CacheException;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.test.utils.TestUtils.humanReadableByteCount;
import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.isMemberNode;
import static com.hazelcast.stabilizer.utils.CommonUtils.sleepSeconds;

public class ExpiryICacheTest {

    private final static ILogger log = Logger.getLogger(ExpiryICacheTest.class);

    // properties
    public String basename = "ttlicachetest";
    public int threadCount = 3;
    public double maxHeapUsagePercentage = 80;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private ICache<Object, Object> cache;
    private long baseLineUsed;
    private final ExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(Duration.ONE_MINUTE);
    private final AtomicLong operations = new AtomicLong();

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        CacheManager cacheManager;
        if (isMemberNode(targetInstance)) {
            HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
            cacheManager = new HazelcastServerCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        } else {
            HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
            cacheManager = new HazelcastClientCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        }

        CacheConfig<Long, Long> config = new CacheConfig<Long, Long>();
        config.setName(basename);

        try {
            cacheManager.createCache(basename, config);
        } catch (CacheException hack) {
            //temp hack to deal with multiple nodes wanting to make the same cache.
            log.severe(hack);
        }
        cache = (ICache<Object, Object>) cacheManager.getCache(basename);
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private double heapUsedPercentage() {
        long total = Runtime.getRuntime().totalMemory();
        long max = Runtime.getRuntime().maxMemory();
        return (100d * total) / max;
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
        @Override
        public void run() {
            long free = Runtime.getRuntime().freeMemory();
            long total = Runtime.getRuntime().totalMemory();
            baseLineUsed = total - free;
            long maxBytes = Runtime.getRuntime().maxMemory();
            double usedOfMax = 100.0 * ((double) baseLineUsed / (double) maxBytes);

            log.info(basename + " before Init");
            log.info(basename + " free = " + humanReadableByteCount(free, true) + " = " + free);
            log.info(basename + " used = " + humanReadableByteCount(baseLineUsed, true) + " = " + baseLineUsed);
            log.info(basename + " max = " + humanReadableByteCount(maxBytes, true) + " = " + maxBytes);
            log.info(basename + " usedOfMax = " + usedOfMax + "%");

            long iteration = 1;
            Random random = new Random();

            while (!testContext.isStopped()) {
                double usedPercentage = heapUsedPercentage();
                if (usedPercentage >= maxHeapUsagePercentage) {
                    log.info("heap used: " + usedPercentage + " % map.size:" + cache.size());

                    sleepSeconds(10);
                } else {
                    for (int k = 0; k < 1000; k++) {
                        iteration++;
                        if (iteration % 100000 == 0) {
                            log.info("at:" + iteration + " heap used: " + usedPercentage + " % map.size:" + cache.size());
                        }

                        long key = random.nextLong();
                        cache.put(key, 0l, expiryPolicy);

                        if (iteration % logFrequency == 0) {
                            log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                        }

                        if (iteration % performanceUpdateFrequency == 0) {
                            operations.addAndGet(performanceUpdateFrequency);
                        }
                    }
                }
            }
            operations.addAndGet(iteration % performanceUpdateFrequency);

            free = Runtime.getRuntime().freeMemory();
            total = Runtime.getRuntime().totalMemory();
            long nowUsed = total - free;
            maxBytes = Runtime.getRuntime().maxMemory();
            usedOfMax = 100.0 * ((double) nowUsed / (double) maxBytes);

            log.info(basename + " After Init");
            log.info(basename + " map = " + cache.size());
            log.info(basename + " free = " + humanReadableByteCount(free, true) + " = " + free);
            log.info(basename + " used = " + humanReadableByteCount(nowUsed, true) + " = " + nowUsed);
            log.info(basename + " max = " + humanReadableByteCount(maxBytes, true) + " = " + maxBytes);
            log.info(basename + " usedOfMax = " + usedOfMax + "%");
            log.info(basename + " map size:" + cache.size());
        }
    }

    @Verify(global = false)
    public void globalVerify() throws Exception {
        long free = Runtime.getRuntime().freeMemory();
        long total = Runtime.getRuntime().totalMemory();
        long used = total - free;
        long maxBytes = Runtime.getRuntime().maxMemory();
        double usedOfMax = 100.0 * ((double) used / (double) maxBytes);

        log.info(basename + " map = " + cache.size());
        log.info(basename + "free = " + humanReadableByteCount(free, true) + " = " + free);
        log.info(basename + "used = " + humanReadableByteCount(used, true) + " = " + used);
        log.info(basename + "max = " + humanReadableByteCount(maxBytes, true) + " = " + maxBytes);
        log.info(basename + "usedOfMax = " + usedOfMax + "%");
    }

    public static void main(String[] args) throws Throwable {
        new TestRunner<ExpiryICacheTest>(new ExpiryICacheTest()).run();
    }
}
