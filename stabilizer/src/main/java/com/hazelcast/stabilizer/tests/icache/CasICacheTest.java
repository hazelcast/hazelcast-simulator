package com.hazelcast.stabilizer.tests.icache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.HazelcastClientCacheManager;
import com.hazelcast.client.cache.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

/**
 * This tests the cas method: replace. So for optimistic concurrency control.
 * <p/>
 * We have a bunch of predefined keys, and we are going to concurrently increment the value
 * and we protect ourselves against lost updates using cas method replace.
 * <p/>
 * Locally we keep track of all increments, and if the sum of these local increments matches the
 * global increment, we are done
 */
public class CasICacheTest {

    private final static ILogger log = Logger.getLogger(CasICacheTest.class);

    public int threadCount = 10;
    public int keyCount = 1000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;

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
        CacheConfig<Integer, Long> config = new CacheConfig<Integer, Long>();
        config.setName(basename);
        config.setTypes(Integer.class, Long.class);

        cacheManager.createCache(basename, config);
        ICache cache = cacheManager.getCache(basename, Integer.class, Long.class);

        for (int k = 0; k < keyCount; k++) {
            cache.put(k, 0l);
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
        private final long[] increments = new long[keyCount];
        ICache<Integer, Long> cache = cacheManager.getCache(basename, Integer.class, Long.class);

        public void run() {
            long iteration = 0;
            while (!testContext.isStopped()) {
                int key = random.nextInt(keyCount);
                long increment = random.nextInt(100);

                for (; ; ) {
                    Long current = cache.get(key);
                    if (cache.replace(key, current, current + increment)) {
                        increments[key] += increment;
                        break;
                    }
                }

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }

                iteration++;
            }
            targetInstance.getList(basename).add(increments);
        }
    }

    @Verify
    public void verify() throws Exception {
        long[] amount = new long[keyCount];

        IList<long[]>resultsPerWorker = targetInstance.getList(basename);
        for (long[] incrments : resultsPerWorker) {
            for (int i=0 ; i<keyCount; i++) {
                amount[i] += incrments[i];
            }
        }

        ICache<Integer, Long> cache = cacheManager.getCache(basename, Integer.class, Long.class);
        int failures = 0;
        for (int k = 0; k < keyCount; k++) {
            long expected = amount[k];
            long found = cache.get(k);
            if (expected != found) {
                failures++;
            }
        }
        assertEquals(failures + " key=>values have been incremented unExpected", 0, failures);
    }
}