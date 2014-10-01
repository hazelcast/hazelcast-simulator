package com.hazelcast.stabilizer.tests.icache;

import com.hazelcast.cache.impl.HazelcastCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.Random;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.sleepMs;
import static org.junit.Assert.assertEquals;


/**
 * In This test was are using the EntryProcessor to increment a key value pair contained in a map
 * we can configure a entry processor delay controlling how long the task lasts, could we over flow hz internal queues
 * we track all the incrementing entry processor task we have submitted and so
 * verify the cache key value pair has been incremented the correct number of times
 */
public class EntryProcessorICacheTest {

    private final static ILogger log = Logger.getLogger(EntryProcessorICacheTest.class);

    public int threadCount = 10;
    public int keyCount = 1000;
    public int minProcessorDelayMs = 0;
    public int maxProcessorDelayMs = 0;

    private int maxIncrement = 100;
    private int completeTaskDelayMs = 8000;

    private Cache<Integer, Long> cache;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private String basename;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        basename = testContext.getTestId();

        HazelcastCacheManager cacheManager;
        if (TestUtils.isMemberNode(targetInstance)) {
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

        cacheManager.createCache(basename, config);
        cache = cacheManager.getCache(basename, Integer.class, Long.class);
    }

    @Teardown(global = true)
    public void teardown() throws Exception {
        cache.close();
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

    private class Worker implements Runnable {
        private final Random random = new Random();
        private final Long[] increments = new Long[keyCount];

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                int key = random.nextInt(keyCount);
                int increment = random.nextInt(maxIncrement);
                int delayMs=0;
                if(maxProcessorDelayMs > 0){
                    delayMs = random.nextInt(maxProcessorDelayMs - minProcessorDelayMs) + minProcessorDelayMs;
                }

                cache.invoke(key, new IncrementEntryProcessor(increment, delayMs));
                increments[key] += increment;
            }

            //sleep to give time for the last EntryProcessor tasks to complete.
            sleepMs(completeTaskDelayMs);
            targetInstance.getList(basename).add(increments);
        }
    }

    @Verify
    public void verify() throws Exception {

        IList<long[]> allIncrements = targetInstance.getList(basename);
        long[] total = new long[keyCount];
        for (long[] increments : allIncrements) {
            for (int i = 0; i < increments.length; i++) {
                total[i] += increments[i];
            }
        }
        log.info(basename + ": collected increments from " + allIncrements.size() + " worker threads");


        int failures = 0;
        for (int i = 0; i < keyCount; i++) {
            if (total[i] != cache.get(i)) {
                failures++;
                log.info(basename + ": key=" + i + " expected val " + total[i] + " !=  cache val" + cache.get(i));
            }
        }
        assertEquals(basename + ": " + failures + " keys have been incremented unexpectedly out of " + keyCount + " keys", 0, failures);
    }

    private static class IncrementEntryProcessor implements EntryProcessor<Integer, Long, Object> , Serializable {
        private final int increment;
        private final int delayMs;

        private IncrementEntryProcessor(int increment, int delayMs) {
            this.increment = increment;
            this.delayMs = delayMs;
        }

        @Override
        public Object process(MutableEntry<Integer, Long> entry, Object... arguments) throws EntryProcessorException {
            sleepMs(delayMs);
            long newValue = entry.getValue() + increment;
            entry.setValue(newValue);
            return null;
        }
    }
}