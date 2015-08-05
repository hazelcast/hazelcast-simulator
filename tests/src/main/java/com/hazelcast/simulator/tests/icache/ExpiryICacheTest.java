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
package com.hazelcast.simulator.tests.icache;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.utils.ThreadSpawner;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;
import static com.hazelcast.simulator.utils.CommonUtils.humanReadableByteCount;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

public class ExpiryICacheTest {

    private static final ILogger LOGGER = Logger.getLogger(ExpiryICacheTest.class);

    // properties
    public String basename = ExpiryICacheTest.class.getSimpleName();
    public int threadCount = 3;
    public double maxHeapUsagePercentage = 80;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;

    private TestContext testContext;
    private ICache<Object, Object> cache;
    private final ExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(Duration.ONE_MINUTE);
    private final AtomicLong operations = new AtomicLong();

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
        CacheManager cacheManager = createCacheManager(hazelcastInstance);

        CacheConfig<Long, Long> config = new CacheConfig<Long, Long>();
        config.setName(basename);

        try {
            cacheManager.createCache(basename, config);
        } catch (CacheException hack) {
            // temp hack to deal with multiple nodes wanting to make the same cache
            LOGGER.severe(hack);
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
            long baseLineUsed = total - free;
            long maxBytes = Runtime.getRuntime().maxMemory();
            double usedOfMax = 100.0 * ((double) baseLineUsed / (double) maxBytes);

            LOGGER.info(basename + " before Init");
            LOGGER.info(basename + " free = " + humanReadableByteCount(free, true) + " = " + free);
            LOGGER.info(basename + " used = " + humanReadableByteCount(baseLineUsed, true) + " = " + baseLineUsed);
            LOGGER.info(basename + " max = " + humanReadableByteCount(maxBytes, true) + " = " + maxBytes);
            LOGGER.info(basename + " usedOfMax = " + usedOfMax + "%");

            long iteration = 1;
            Random random = new Random();

            while (!testContext.isStopped()) {
                double usedPercentage = heapUsedPercentage();
                if (usedPercentage >= maxHeapUsagePercentage) {
                    LOGGER.info("heap used: " + usedPercentage + " % map.size:" + cache.size());

                    sleepSeconds(10);
                } else {
                    for (int k = 0; k < 1000; k++) {
                        iteration++;
                        if (iteration % 100000 == 0) {
                            LOGGER.info("at:" + iteration + " heap used: " + usedPercentage + " % map.size:" + cache.size());
                        }

                        long key = random.nextLong();
                        cache.put(key, 0L, expiryPolicy);

                        if (iteration % logFrequency == 0) {
                            LOGGER.info(Thread.currentThread().getName() + " At iteration: " + iteration);
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

            LOGGER.info(basename + " After Init");
            LOGGER.info(basename + " map = " + cache.size());
            LOGGER.info(basename + " free = " + humanReadableByteCount(free, true) + " = " + free);
            LOGGER.info(basename + " used = " + humanReadableByteCount(nowUsed, true) + " = " + nowUsed);
            LOGGER.info(basename + " max = " + humanReadableByteCount(maxBytes, true) + " = " + maxBytes);
            LOGGER.info(basename + " usedOfMax = " + usedOfMax + "%");
            LOGGER.info(basename + " map size:" + cache.size());
        }
    }

    @Verify(global = false)
    public void globalVerify() throws Exception {
        long free = Runtime.getRuntime().freeMemory();
        long total = Runtime.getRuntime().totalMemory();
        long used = total - free;
        long maxBytes = Runtime.getRuntime().maxMemory();
        double usedOfMax = 100.0 * ((double) used / (double) maxBytes);

        LOGGER.info(basename + " map = " + cache.size());
        LOGGER.info(basename + "free = " + humanReadableByteCount(free, true) + " = " + free);
        LOGGER.info(basename + "used = " + humanReadableByteCount(used, true) + " = " + used);
        LOGGER.info(basename + "max = " + humanReadableByteCount(maxBytes, true) + " = " + maxBytes);
        LOGGER.info(basename + "usedOfMax = " + usedOfMax + "%");
    }

    public static void main(String[] args) throws Exception {
        new TestRunner<ExpiryICacheTest>(new ExpiryICacheTest()).run();
    }
}
