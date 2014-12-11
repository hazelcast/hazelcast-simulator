package com.hazelcast.stabilizer.tests.icache;


import com.hazelcast.cache.ICache;

import javax.cache.CacheManager;;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
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
import com.hazelcast.stabilizer.worker.OperationSelector;

import javax.cache.CacheException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This test demonstrates effect of batching. It uses async methods to invoke operation and wait for future
 * to complete every {@code batchSize} invocations. Hence setting batchSize to 1 is effectively the same as
 * using sync operations.
 *
 * {@code batchSize > 1} causes batch-effect to kick-in, pipe-lines are utilized better
 * and overall throughput goes up.
 */
public class BatchingICacheTest {

    private final static ILogger log = Logger.getLogger(PerformanceICacheTest.class);

    //props
    public int threadCount = 10;
    public int keyCount = 1000000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    public String basename = getClass().getSimpleName().toLowerCase();
    public double writeProbability = 0.1;
    public int batchSize = 1;

    private ICache<Object, Object> cache;
    private final AtomicLong operations = new AtomicLong();
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private OperationSelector<Operation> selector = new OperationSelector<Operation>();

    @Setup
    public void setup(TestContext testContext) throws Exception {
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

        CacheConfig<Integer, Integer> config = new CacheConfig<Integer, Integer>();
        config.setName(basename);

        try {
            cacheManager.createCache(basename, config);
        } catch (CacheException hack) {
            //temp hack to deal with multiple nodes wanting to make the same cache.
            log.severe(hack);
        }
        cache = (ICache<Object, Object>) cacheManager.getCache(basename);

        selector.addOperation(Operation.PUT, writeProbability)
                .empty(Operation.GET);
    }

    @Teardown
    public void teardown() throws Exception {
        cache.close();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        for (int k = 0; k < keyCount; k++) {
            cache.put(k, 0);

            if (k % 10000 == 0) {
                log.info("Warmup: " + k);
            }
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
        private final List<ICompletableFuture<?>> futureList = new ArrayList<ICompletableFuture<?>>(batchSize);

        @Override
        public void run() {
            long iteration = 0;
            while (!testContext.isStopped()) {
                ICompletableFuture<?> future = selectAndInvokeOperation();
                futureList.add(future);

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
                iteration++;
                syncIfNecessary(iteration);
            }
        }

        private ICompletableFuture<?> selectAndInvokeOperation() {
            Integer key = random.nextInt(keyCount);
            Operation operation = selector.select();
            switch (operation) {
                case PUT:
                    Integer value = random.nextInt();
                    return cache.putAsync(key, value);
                case GET:
                    return cache.getAsync(key);
                default:
                    throw new RuntimeException("Unknown operation '" + operation + "' selected.");
            }
        }

        private void syncIfNecessary(long iteration) {
            if (iteration % batchSize == 0) {
                for (ICompletableFuture<?> f : futureList) {
                    try {
                        f.get();
                    } catch (RuntimeException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                futureList.clear();
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        PerformanceICacheTest test = new PerformanceICacheTest();
        new TestRunner(test).run();
    }

    static enum Operation {
        PUT,
        GET,
    }
}
