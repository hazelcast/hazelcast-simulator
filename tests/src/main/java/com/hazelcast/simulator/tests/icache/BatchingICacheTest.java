package com.hazelcast.simulator.tests.icache;

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.test.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.selector.OperationSelector;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;

/**
 * This test demonstrates effect of batching. It uses async methods to invoke operation and wait for future
 * to complete every {@code batchSize} invocations. Hence setting batchSize to 1 is effectively the same as
 * using sync operations.
 *
 * {@code batchSize > 1} causes batch-effect to kick-in, pipe-lines are utilized better
 * and overall throughput goes up.
 */
public class BatchingICacheTest {

    private enum Operation {
        PUT,
        GET,
    }

    private static final ILogger log = Logger.getLogger(PerformanceICacheTest.class);

    //props
    public int threadCount = 10;
    public int keyCount = 1000000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    public String basename = getClass().getSimpleName().toLowerCase();
    public double writeProb = 0.1;
    public int batchSize = 1;

    private ICache<Object, Object> cache;
    private final AtomicLong operations = new AtomicLong();
    private TestContext testContext;
    private OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;

        HazelcastInstance targetInstance = testContext.getTargetInstance();
        CacheManager cacheManager;
        if (isMemberNode(targetInstance)) {
            HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
            cacheManager = new HazelcastServerCacheManager(hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(),
                    null);
        } else {
            HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
            cacheManager = new HazelcastClientCacheManager(hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(),
                    null);
        }

        CacheConfig<Integer, Integer> config = new CacheConfig<Integer, Integer>();
        config.setName(basename);

        try {
            cacheManager.createCache(basename, config);
        } catch (CacheException hack) {
            // temp hack to deal with multiple nodes wanting to make the same cache
            log.severe(hack);
        }
        cache = (ICache<Object, Object>) cacheManager.getCache(basename);

        operationSelectorBuilder.addOperation(Operation.PUT, writeProb).addDefaultOperation(Operation.GET);
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
        private final OperationSelector<Operation> selector = operationSelectorBuilder.build();
        private final Random random = new Random();
        private final List<ICompletableFuture<?>> futureList = new ArrayList<ICompletableFuture<?>>(batchSize);

        @Override
        public void run() {
            long iteration = 0;
            while (!testContext.isStopped()) {
                ICompletableFuture<?> future = selectAndInvokeOperation();
                futureList.add(future);

                iteration++;
                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }
                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }

                syncIfNecessary(iteration);
            }
            operations.addAndGet(iteration % performanceUpdateFrequency);
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
        new TestRunner<PerformanceICacheTest>(test).run();
    }
}
