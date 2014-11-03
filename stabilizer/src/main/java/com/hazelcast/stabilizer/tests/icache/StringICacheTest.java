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

import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
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
import com.hazelcast.stabilizer.tests.utils.KeyLocality;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.tests.map.helpers.KeyUtils.generateKeys;
import static com.hazelcast.stabilizer.tests.utils.TestUtils.waitClusterSize;

public class StringICacheTest {

    private final static ILogger log = Logger.getLogger(StringICacheTest.class);

    //props
    public int writePercentage = 10;
    public int threadCount = 10;
    public int keyLength = 10;
    public int valueLength = 10;
    public int keyCount = 10000;
    public int valueCount = 10000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    // if we use the putAndGet (so returning a value) or the put (which returns void)
    public boolean useGetAndPut = true;
    public String basename = "stringicache";
    public KeyLocality keyLocality = KeyLocality.Random;
    public int minNumberOfMembers = 0;

    private Cache<String, String> cache;
    private String[] keys;
    private String[] values;
    private final AtomicLong operations = new AtomicLong();
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        if (writePercentage < 0) {
            throw new IllegalArgumentException("Write percentage can't be smaller than 0");
        }

        if (writePercentage > 100) {
            throw new IllegalArgumentException("Write percentage can't be larger than 100");
        }

        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

        CacheManager cacheManager;
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

        try {
            cacheManager.createCache(basename, config);
        } catch (CacheException hack) {
            //temp hack to deal with multiple nodes wanting to make the same cache.
            log.severe(hack);
        }
        cache = cacheManager.getCache(basename);
    }

    @Teardown
    public void teardown() throws Exception {
        cache.close();
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        waitClusterSize(log, targetInstance, minNumberOfMembers);

        keys = generateKeys(keyCount, keyLength, keyLocality, testContext.getTargetInstance());
        values = StringUtils.generateStrings(valueCount, valueLength);

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

        @Override
        public void run() {
            long iteration = 0;
            while (!testContext.isStopped()) {

                String key = randomKey();

                if (shouldWrite(iteration)) {
                    String value = randomValue();
                    if (useGetAndPut) {
                        cache.getAndPut(key, value);
                    } else {
                        cache.put(key, value);
                    }
                } else {
                    cache.get(key);
                }

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }

                iteration++;
            }
        }

        private String randomValue() {
            return values[random.nextInt(values.length)];
        }

        private String randomKey() {
            int length = keys.length;
            return keys[random.nextInt(length)];
        }

        private boolean shouldWrite(long iteration) {
            if (writePercentage == 0) {
                return false;
            } else if (writePercentage == 100) {
                return true;
            } else {
                return (iteration % 100) < writePercentage;
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        StringICacheTest test = new StringICacheTest();
        test.writePercentage = 10;
        new TestRunner(test).run();
    }
}
