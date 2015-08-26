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
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;

public class EntryProcessorICacheTest {

    private static final ILogger LOGGER = Logger.getLogger(EntryProcessorICacheTest.class);

    // properties
    public String basename = EntryProcessorICacheTest.class.getSimpleName();
    public int threadCount = 10;
    public int keyCount = 1000;
    public int minProcessorDelayMs = 0;
    public int maxProcessorDelayMs = 0;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;

    private final AtomicLong operations = new AtomicLong();
    private Cache<Integer, Long> cache;
    private IList<Map<Integer, Long>> resultsPerWorker;
    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();

        CacheManager cacheManager = createCacheManager(hazelcastInstance);

        CacheConfig<Integer, Long> config = new CacheConfig<Integer, Long>();
        config.setName(basename);

        try {
            cacheManager.createCache(basename, config);
        } catch (CacheException hack) {
            // temp hack to deal with multiple nodes wanting to make the same cache
            LOGGER.severe(hack);
        }

        cache = cacheManager.getCache(basename);
        resultsPerWorker = hazelcastInstance.getList(basename + "ResultMap" + testContext.getTestId());
    }

    @Teardown
    public void teardown() throws Exception {
        cache.close();
        resultsPerWorker.destroy();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        for (int i = 0; i < keyCount; i++) {
            cache.put(i, 0L);
        }
    }

    @Verify
    public void verify() throws Exception {
        long[] amount = new long[keyCount];

        for (Map<Integer, Long> map : resultsPerWorker) {
            for (Map.Entry<Integer, Long> entry : map.entrySet()) {
                amount[entry.getKey()] += entry.getValue();
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

        assertEquals("Failures have been found", 0, failures);
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int i = 0; i < threadCount; i++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();
        private final Map<Integer, Long> result = new HashMap<Integer, Long>();

        public Worker() {
            for (int i = 0; i < keyCount; i++) {
                result.put(i, 0L);
            }
        }

        @Override
        public void run() {
            long iteration = 0;
            while (!testContext.isStopped()) {
                int key = random.nextInt(keyCount);
                long increment = random.nextInt(100);

                int delayMs = 0;
                if (maxProcessorDelayMs != 0) {
                    delayMs = minProcessorDelayMs + random.nextInt(maxProcessorDelayMs);
                }

                cache.invoke(key, new IncrementEntryProcessor(increment, delayMs));
                increment(key, increment);

                iteration++;
                if (iteration % logFrequency == 0) {
                    LOGGER.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }
                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
            }
            operations.addAndGet(iteration % performanceUpdateFrequency);

            // sleep to give time for the last EntryProcessor tasks to complete
            sleepMillis(maxProcessorDelayMs * 2);
            resultsPerWorker.add(result);
        }

        private void increment(int key, long increment) {
            result.put(key, result.get(key) + increment);
        }
    }

    private static final class IncrementEntryProcessor implements EntryProcessor<Integer, Long, Object>, Serializable {

        private final long increment;
        private final int delayMs;

        private IncrementEntryProcessor(long increment, int delayMs) {
            this.increment = increment;
            this.delayMs = delayMs;
        }

        @Override
        public Object process(MutableEntry<Integer, Long> entry, Object... arguments) throws EntryProcessorException {
            if (delayMs > 0) {
                sleepMillis(delayMs);
            }
            long newValue = entry.getValue() + increment;
            entry.setValue(newValue);
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        EntryProcessorICacheTest test = new EntryProcessorICacheTest();
        new TestRunner<EntryProcessorICacheTest>(test).run();
    }
}
