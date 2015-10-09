package com.hazelcast.simulator.tests.icache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;

/**
 * A performance test for the cache. The key is integer and value is a integer
 */
public class PerformanceICacheTest {

    private static final ILogger LOGGER = Logger.getLogger(PerformanceICacheTest.class);

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public String basename = PerformanceICacheTest.class.getSimpleName();
    public int keyCount = 1000000;
    public double putProb = 0.1;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private Cache<Object, Object> cache;

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
        cache = cacheManager.getCache(basename);

        operationSelectorBuilder.addOperation(Operation.PUT, putProb)
                .addDefaultOperation(Operation.GET);
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

        private int value;

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        public void timeStep(Operation operation) {
            Integer key = randomInt(keyCount);
            switch (operation) {
                case PUT:
                    cache.put(key, value++);
                    break;
                case GET:
                    cache.get(key);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        PerformanceICacheTest test = new PerformanceICacheTest();
        new TestRunner<PerformanceICacheTest>(test).run();
    }
}
