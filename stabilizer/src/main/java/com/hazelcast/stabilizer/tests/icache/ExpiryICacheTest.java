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
import com.hazelcast.cache.impl.HazelcastCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.HazelcastClientCacheManager;
import com.hazelcast.client.cache.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import javax.cache.CacheException;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.humanReadableByteCount;
import static com.hazelcast.stabilizer.tests.utils.TestUtils.sleepMs;

public class ExpiryICacheTest {

    private final static ILogger log = Logger.getLogger(ExpiryICacheTest.class);

    public int threadCount = 3;
    public double maxHeapUsagePercentage = 80;
    public int performanceUpdateFrequency = 10000;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    public String basename;

    private HazelcastCacheManager cacheManager;
    private final ExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(Duration.ONE_MINUTE);
    private final AtomicLong operations = new AtomicLong();

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
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
    }

    @Warmup(global = true)
    public void warmup( ) throws Exception {
        CacheConfig<Long, Long> config = new CacheConfig<Long, Long>();
        config.setName(basename);
        config.setTypes(Long.class, Long.class);
        cacheManager.createCache(basename, config);
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
            ICache<Long, Long> cache = cacheManager.getCache(basename, Long.class, Long.class);

            TestUtils.logMemStats(log, basename);
            log.info(basename + " map = " + cache.size());

            long iteration = 1;
            Random random = new Random();

            while (!testContext.isStopped()) {
                double usedPercentage = heapUsedPercentage();
                if (usedPercentage >= maxHeapUsagePercentage) {
                    log.info("heap used: " + usedPercentage + " % map.size:" + cache.size());

                    sleepMs(10000);
                } else {
                    for (int k = 0; k < 1000; k++) {
                        iteration++;
                        if (iteration % 100000 == 0) {
                            log.info("at:" + iteration + " heap used: " + usedPercentage + " % map.size:" + cache.size());
                        }
                        long key = random.nextLong();
                        cache.put(key, 0l, expiryPolicy);


                        if (iteration % performanceUpdateFrequency == 0) {
                            operations.addAndGet(performanceUpdateFrequency);
                        }
                    }
                }
            }
            TestUtils.logMemStats(log, basename);
            log.info(basename + " map = " + cache.size());
        }
    }

    @Verify(global = false)
    public void globalVerify() throws Exception {
        ICache<Long, Long> cache = cacheManager.getCache(basename, Long.class, Long.class);
        TestUtils.logMemStats(log, basename);
        log.info(basename + " map = " + cache.size());
    }

    public static void main(String[] args) throws Throwable {
        new TestRunner(new ExpiryICacheTest()).run();
    }
}
