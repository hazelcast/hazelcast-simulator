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
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import com.hazelcast.util.EmptyStatement;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FormatUtils.humanReadableByteCount;

public class ExpiryICacheTest {

    private static final ILogger LOGGER = Logger.getLogger(ExpiryICacheTest.class);

    // properties
    public String basename = ExpiryICacheTest.class.getSimpleName();
    public double maxHeapUsagePercentage = 80;

    private final ExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(Duration.ONE_MINUTE);

    private ICache<Long, Long> cache;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
        CacheManager cacheManager = createCacheManager(hazelcastInstance);

        CacheConfig<Long, Long> config = new CacheConfig<Long, Long>();
        config.setName(basename);
        try {
            cacheManager.createCache(basename, config);
        } catch (CacheException e) {
            // ignore exception when multiple nodes created the same cache
            EmptyStatement.ignore(e);
        }

        cache = (ICache<Long, Long>) cacheManager.<Long, Long>getCache(basename);
    }

    @Verify(global = false)
    public void globalVerify() throws Exception {
        LOGGER.info(basename + " cache size = " + cache.size());
        logMemoryStatistics();
    }

    @RunWithWorker
    public Worker run() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        @Override
        protected void beforeRun() {
            LOGGER.info(basename + ".beforeRun()");
            logMemoryStatistics();
        }

        @Override
        public void timeStep() {
            double usedPercentage = heapUsedPercentage();
            if (usedPercentage >= maxHeapUsagePercentage) {
                LOGGER.info("heap used: " + usedPercentage + "% cache size: " + cache.size());
                sleepSeconds(10);
            } else {
                for (int i = 0; i < 1000; i++) {
                    if (getIteration() % 100000 == 0) {
                        LOGGER.info("At " + getIteration() + " heap used: " + usedPercentage + "% cache size: " + cache.size());
                    }

                    long key = getRandom().nextLong();
                    cache.put(key, 0L, expiryPolicy);
                }
            }
        }

        @Override
        protected void afterRun() {
            LOGGER.info(basename + ".afterRun()");
            logMemoryStatistics();
        }

        private double heapUsedPercentage() {
            long total = Runtime.getRuntime().totalMemory();
            long max = Runtime.getRuntime().maxMemory();
            return (100d * total) / max;
        }
    }

    private void logMemoryStatistics() {
        long free = Runtime.getRuntime().freeMemory();
        long total = Runtime.getRuntime().totalMemory();
        long baseLineUsed = total - free;
        long maxBytes = Runtime.getRuntime().maxMemory();
        double usedOfMax = 100.0 * ((double) baseLineUsed / (double) maxBytes);

        LOGGER.info(basename + " free = " + humanReadableByteCount(free, true) + " = " + free);
        LOGGER.info(basename + " used = " + humanReadableByteCount(baseLineUsed, true) + " = " + baseLineUsed);
        LOGGER.info(basename + " max = " + humanReadableByteCount(maxBytes, true) + " = " + maxBytes);
        LOGGER.info(basename + " usedOfMax = " + usedOfMax + "%");
    }

    public static void main(String[] args) throws Exception {
        new TestRunner<ExpiryICacheTest>(new ExpiryICacheTest()).run();
    }
}
