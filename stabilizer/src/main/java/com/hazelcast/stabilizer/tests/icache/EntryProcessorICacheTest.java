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

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class EntryProcessorICacheTest {

    private final static ILogger log = Logger.getLogger(EntryProcessorICacheTest.class);

    public int threadCount = 10;
    public int keyCount = 1000;
    public int minProcessorDelayMs = 0;
    public int maxProcessorDelayMs = 0;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;

    private final AtomicLong operations = new AtomicLong();
    private ICache<Integer, Long> cache;
    private IList<long[]> resultsPerWorker;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private HazelcastCacheManager cacheManager;
    private String basename;


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
        resultsPerWorker = targetInstance.getList(basename);
    }

    @Teardown
    public void teardown() throws Exception {
        cache.close();
        resultsPerWorker.destroy();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {

        CacheConfig<Integer, Long> config = new CacheConfig<Integer, Long>();
        config.setName(basename);
        config.setTypes(Integer.class, Long.class);

        cacheManager.createCache(basename, config);
        cache = cacheManager.getCache(basename);

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

        public void run() {
            long iteration = 0;
            cache = cacheManager.getCache(basename);

            while (!testContext.isStopped()) {
                int key = random.nextInt(keyCount);
                long increment = random.nextInt(100);

                int delayMs = 0;
                if (maxProcessorDelayMs != 0) {
                    delayMs = minProcessorDelayMs + random.nextInt(maxProcessorDelayMs);
                }

                cache.invoke(key, new IncrementEntryProcessor(increment, delayMs));
                increments[key] += increment;

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
                iteration++;
            }

            //sleep to give time for the last EntryProcessor tasks to complete.
            try {
                Thread.sleep(maxProcessorDelayMs * 2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            resultsPerWorker.add(increments);
        }
    }

    @Verify
    public void verify() throws Exception {
        long[] expected = new long[keyCount];

        for (long[] increments : resultsPerWorker) {
            for (int i=0; i<keyCount; i++) {
                expected[i] += increments[i];
            }
        }

        int failures = 0;
        for (int k = 0; k < keyCount; k++) {
            if (expected[k] != cache.get(k)) {
                failures++;
                log.info(basename + ": key=" + k + " expected " + expected[k] + " != " + "actual " + cache.get(k));
            }
        }
        assertEquals(basename + ": " + failures + " key=>values have been incremented unExpected", 0, failures);
    }

    private static class IncrementEntryProcessor implements EntryProcessor<Integer, Long, Object> , Serializable {
        private final long increment;
        private final long delayMs;

        private IncrementEntryProcessor(long increment, long delayMs) {
            this.increment = increment;
            this.delayMs = delayMs;
        }

        @Override
        public Object process(MutableEntry<Integer, Long> entry, Object... arguments) throws EntryProcessorException {
            delay();
            long newValue = entry.getValue() + increment;
            entry.setValue(newValue);
            return null;
        }

        private void delay() {
            if (delayMs != 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        EntryProcessorICacheTest test = new EntryProcessorICacheTest();
        new TestRunner(test).run();
    }
}

