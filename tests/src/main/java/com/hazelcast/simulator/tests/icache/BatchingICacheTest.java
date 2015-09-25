package com.hazelcast.simulator.tests.icache;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.selector.OperationSelector;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;

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

    private static final ILogger LOGGER = Logger.getLogger(PerformanceICacheTest.class);

    // properties
    public int threadCount = 10;
    public String basename = BatchingICacheTest.class.getSimpleName();
    public int keyCount = 1000000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    public double writeProb = 0.1;
    public int batchSize = 1;

    private ICache<Object, Object> cache;
    private final AtomicLong operations = new AtomicLong();
    private TestContext testContext;
    private OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;

        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
        CacheManager cacheManager = createCacheManager(hazelcastInstance);

        CacheConfig<Integer, Integer> config = new CacheConfig<Integer, Integer>();
        config.setName(basename);

        try {
            cacheManager.createCache(basename, config);
        } catch (CacheException hack) {
            // temp hack to deal with multiple nodes wanting to make the same cache
            LOGGER.severe(hack);
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
        Streamer<Object, Object> streamer = StreamerFactory.getInstance(cache);
        for (int k = 0; k < keyCount; k++) {
            streamer.pushEntry(k, 0);

            if (k % 10000 == 0) {
                LOGGER.info("Warmup: " + k);
            }
        }
        streamer.await();
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
                    LOGGER.info(Thread.currentThread().getName() + " At iteration: " + iteration);
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
                    throw new UnsupportedOperationException("Unknown operation " + operation);
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
                        throw new TestException(e);
                    }
                }
                futureList.clear();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        PerformanceICacheTest test = new PerformanceICacheTest();
        new TestRunner<PerformanceICacheTest>(test).run();
    }
}
