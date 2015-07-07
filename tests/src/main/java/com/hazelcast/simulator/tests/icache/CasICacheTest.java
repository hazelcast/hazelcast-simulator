package com.hazelcast.simulator.tests.icache;

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
import com.hazelcast.simulator.utils.ThreadSpawner;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;
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

    private static final ILogger LOGGER = Logger.getLogger(CasICacheTest.class);

    public String basename;
    public int threadCount = 10;
    public int keyCount = 1000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;

    private final AtomicLong operations = new AtomicLong();
    private IList<long[]> resultsPerWorker;
    private TestContext testContext;
    private Cache<Integer, Long> cache;
    private String basename;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
        basename = testContext.getTestId();
        resultsPerWorker = hazelcastInstance.getList(basename);

        CacheManager cacheManager = createCacheManager(hazelcastInstance);

        CacheConfig<Integer, Long> config = new CacheConfig<Integer, Long>();
        config.setName(basename);

        try {
            cacheManager.createCache(basename, config);
        } catch (CacheException e) {
            LOGGER.severe(basename + ": createCache " + e);
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
            cache.put(k, 0L);
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

        for (long[] increments : resultsPerWorker) {
            for (int i = 0; i < keyCount; i++) {
                amount[i] += increments[i];
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
                    LOGGER.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }
                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
            }
            operations.addAndGet(iteration % performanceUpdateFrequency);
            resultsPerWorker.add(increments);
        }
    }

    public static void main(String[] args) throws Exception {
        CasICacheTest test = new CasICacheTest();
        new TestRunner<CasICacheTest>(test).run();
    }
}
