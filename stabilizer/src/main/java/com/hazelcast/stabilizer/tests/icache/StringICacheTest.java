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
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.map.helpers.StringUtils;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import javax.cache.CacheException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class StringICacheTest {

    private final static ILogger log = Logger.getLogger(StringICacheTest.class);

    public int threadCount = 10;
    public double getProb=0.5;
    public double putProb=0.25;
    public double getAndPutProb=0.25;

    public int keyLength = 10;
    public int valueLength = 10;
    public int keyCount = 10000;
    public int valueCount = 10000;

    public int performanceUpdateFrequency = 10000;

    public boolean remoteKeysOnly = false;
    public int minNumberOfMembers = 0;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private String basename;

    private HazelcastCacheManager cacheManager;
    private ICache<String, String> cache;
    private String[] keys;
    private String[] values;
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

        CacheConfig<String, String> config = new CacheConfig<String, String>();
        config.setName(basename);
        config.setTypes(String.class, String.class);

        try {
            cacheManager.createCache(basename, config);
        } catch (CacheException e) {
            log.severe(basename+" :"+e, e);
        }
        cache = cacheManager.getCache(basename, String.class, String.class);
    }

    @Teardown
    public void teardown() throws Exception {
        cache.close();
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        TestUtils.waitClusterSize(log, targetInstance, minNumberOfMembers);
        TestUtils.warmupPartitions(log, targetInstance);

        keys = new String[keyCount];
        for (int k = 0; k < keys.length; k++) {
            keys[k] = StringUtils.generateKey(keyLength, remoteKeysOnly, testContext.getTargetInstance());
        }

        values = new String[valueCount];
        for (int k = 0; k < values.length; k++) {
            values[k] = StringUtils.makeString(valueLength);
        }

        Random random = new Random();

        for (int k = 0; k < keys.length; k++) {
            String key = keys[random.nextInt(keyCount)];
            String value = values[random.nextInt(valueCount)];
            cache.put(key, value);
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        public void run() {
            long iteration = 0;
            while (!testContext.isStopped()) {

                String key = values[random.nextInt(keyCount)];
                String value = keys[random.nextInt(valueCount)];

                double chance = random.nextDouble();
                if ((chance -= getProb) < 0) {
                    cache.get(key);

                } else if ((chance -= putProb) < 0) {
                    cache.put(key, value);

                } else if ((chance -= getAndPutProb) < 0) {
                    cache.getAndPut(key, value);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
                iteration++;
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        StringICacheTest test = new StringICacheTest();
        new TestRunner(test).run();
    }
}
