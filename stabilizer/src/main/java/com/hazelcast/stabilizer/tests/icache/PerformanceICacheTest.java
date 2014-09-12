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
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import javax.cache.CacheException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A performance test for the cache. The key is integer and value is a integer
 */
public class PerformanceICacheTest {

    public int threadCount = 10;
    public int keyCount = 1000000;
    public int performanceUpdateFrequency = 10000;

    public double getProb = 0.9;
    public double putProb = 0.1;

    private final AtomicLong operations = new AtomicLong();
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private HazelcastCacheManager cacheManager;
    private String basename;

    @Setup
    public void setup(TestContext testContext) throws Exception {

        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        basename=testContext.getTestId();

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
    public void warmup() throws Exception {

        CacheConfig<Integer, Integer> config = new CacheConfig<Integer, Integer>();
        config.setName(basename);
        config.setTypes(Integer.class, Integer.class);

        cacheManager.createCache(basename, config);
        ICache<Integer, Integer> cache = cacheManager.getCache(basename, Integer.class, Integer.class);

        for (int k = 0; k < keyCount; k++) {
            cache.put(k, 0);
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
            ICache<Integer, Integer> cache = cacheManager.getCache(basename, Integer.class, Integer.class);

            long iteration = 0;
            while (!testContext.isStopped()) {

                int key = random.nextInt(keyCount);
                double chance = random.nextDouble();
                if ( (chance -= putProb) < 0) {
                    cache.put(key, (int) iteration);
                } else if ((chance -= getProb) < 0) {
                    cache.get(key);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
                iteration++;
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        PerformanceICacheTest test = new PerformanceICacheTest();
        new TestRunner(test).run();
    }
}
