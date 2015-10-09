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
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;

/**
 * Demonstrates the effect of batching.
 *
 * It uses async methods to invoke operation and wait for future to complete every {@code batchSize} invocations.
 * Hence setting {@link #batchSize} to 1 is effectively the same as using sync operations.
 *
 * Setting {@link #batchSize} to values greater than 1 causes the batch-effect to kick-in, pipe-lines are utilized better
 * and overall throughput goes up.
 */
public class BatchingICacheTest {

    private enum Operation {
        PUT,
        GET,
    }

    private static final ILogger LOGGER = Logger.getLogger(PerformanceICacheTest.class);

    // properties
    public int keyCount = 1000000;
    public String basename = BatchingICacheTest.class.getSimpleName();
    public double writeProb = 0.1;
    public int batchSize = 1;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private ICache<Object, Object> cache;

    @Setup
    public void setup(TestContext testContext) throws Exception {
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
        for (int i = 0; i < keyCount; i++) {
            streamer.pushEntry(i, 0);
        }
        streamer.await();
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        private final List<ICompletableFuture<?>> futureList = new ArrayList<ICompletableFuture<?>>(batchSize);

        private long iteration;

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        public void timeStep(Operation operation) {
            Integer key = randomInt(keyCount);
            ICompletableFuture<?> future;
            switch (operation) {
                case PUT:
                    Integer value = randomInt();
                    future = cache.putAsync(key, value);
                    break;
                case GET:
                    future = cache.getAsync(key);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown operation " + operation);
            }
            futureList.add(future);

            syncIfNecessary(iteration++);
        }

        private void syncIfNecessary(long iteration) {
            if (iteration % batchSize == 0) {
                for (ICompletableFuture<?> future : futureList) {
                    try {
                        future.get();
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
