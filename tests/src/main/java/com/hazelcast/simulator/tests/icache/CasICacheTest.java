package com.hazelcast.simulator.tests.icache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;
import static org.junit.Assert.assertEquals;

/**
 * Tests the cas method {@link Cache#replace(Object, Object, Object)} for optimistic concurrency control.
 *
 * With a collection of predefined keys we concurrently increment the value.
 * We protect ourselves against lost updates using the cas method {@link Cache#replace(Object, Object, Object)}.
 *
 * Locally we keep track of all increments. We verify if the sum of these local increments matches the global increment.
 */
public class CasICacheTest {

    private static final ILogger LOGGER = Logger.getLogger(CasICacheTest.class);

    public String basename = CasICacheTest.class.getSimpleName();
    public int keyCount = 1000;

    private Cache<Integer, Long> cache;
    private IList<long[]> resultsPerWorker;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
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
        Streamer<Integer, Long> streamer = StreamerFactory.getInstance(cache);
        for (int i = 0; i < keyCount; i++) {
            streamer.pushEntry(i, 0L);
        }
        streamer.await();
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
        for (int i = 0; i < keyCount; i++) {
            long expected = amount[i];
            long found = cache.get(i);
            if (expected != found) {
                failures++;
            }
        }
        assertEquals(failures + " key=>values have been incremented unExpected", 0, failures);
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        private final long[] increments = new long[keyCount];

        @Override
        public void timeStep() {
            int key = randomInt(keyCount);
            long increment = randomInt(100);

            while (true) {
                Long current = cache.get(key);
                if (cache.replace(key, current, current + increment)) {
                    increments[key] += increment;
                    break;
                }
            }
        }

        @Override
        protected void afterRun() {
            resultsPerWorker.add(increments);
        }
    }

    public static void main(String[] args) throws Exception {
        CasICacheTest test = new CasICacheTest();
        new TestRunner<CasICacheTest>(test).run();
    }
}
