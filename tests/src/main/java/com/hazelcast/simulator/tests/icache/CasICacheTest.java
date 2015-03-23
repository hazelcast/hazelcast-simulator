package com.hazelcast.simulator.tests.icache;

import javax.cache.Cache;
import javax.cache.CacheManager;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.test.utils.ThreadSpawner;

import javax.cache.CacheException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static org.junit.Assert.assertEquals;

/**
 * This tests the cas method: replace. So for optimistic concurrency control.
 *
 * We have a bunch of predefined keys, and we are going to concurrently increment the value
 * and we protect ourselves against lost updates using cas method replace.
 *
 * Locally we keep track of all increments, and if the sum of these local increments matches the
 * global increment, we are done
 */
public class CasICacheTest {

    private static final ILogger log = Logger.getLogger(CasICacheTest.class);

    public int threadCount = 10;
    public int keyCount = 1000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;

    private final AtomicLong operations = new AtomicLong();
    private IList<long[]> resultsPerWorker;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private CacheManager cacheManager;
    private Cache<Integer, Long> cache;
    private String basename;


    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        basename=testContext.getTestId();
        resultsPerWorker = targetInstance.getList(basename);


        if (isMemberNode(targetInstance)) {
            HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
            cacheManager = new HazelcastServerCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        } else {
            HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
            cacheManager = new HazelcastClientCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        }

        CacheConfig<Integer, Long> config = new CacheConfig<Integer, Long>();
        config.setName(basename);

        try {
            cacheManager.createCache(basename, config);
        } catch (CacheException e) {
            log.severe(basename + ": createCache "+e);
        }
        cache = cacheManager.getCache(basename);
    }

    @Teardown
    public void teardown() throws Exception {
        cache.close();
        resultsPerWorker.destroy();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
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

    @Verify
    public void verify() throws Exception {
        long[] amount = new long[keyCount];

        for (long[] incrments : resultsPerWorker) {
            for (int i=0 ; i<keyCount; i++) {
                amount[i] += incrments[i];
            }
        }

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

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();
        private final long[] increments = new long[keyCount];

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

                iteration++;
                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }
                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
            }
            operations.addAndGet(iteration % performanceUpdateFrequency);
            resultsPerWorker.add(increments);
        }
    }

    public static void main(String[] args) throws Throwable {
        CasICacheTest test = new CasICacheTest();
        new TestRunner<CasICacheTest>(test).run();
    }
}