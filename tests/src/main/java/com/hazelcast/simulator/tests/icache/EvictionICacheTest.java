/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.icache;

import com.hazelcast.cache.ICache;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.selector.OperationSelector;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;

import javax.cache.CacheManager;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;
import static org.junit.Assert.fail;

/**
 * This test is expecting to work with an ICache which has a max-size policy and an eviction-policy
 * defined. The run body of the test simply puts random key value pairs to the ICache, and checks
 * the size of the ICache has not grown above the defined max size + a configurable size margin.
 * As the max-size policy is not a hard limit we use a configurable size margin in the verification
 * of the cache size. The test also logs the global max size of the ICache observed from all test
 * participants, providing no assertion errors were throw.
 */
public class EvictionICacheTest {

    private enum Operation {
        PUT,
        PUT_ASYNC,
        PUT_ALL,
    }

    private static final ILogger LOGGER = Logger.getLogger(EvictionICacheTest.class);

    public String basename = EvictionICacheTest.class.getSimpleName();
    public int threadCount = 3;
    public int partitionCount;

    // number of bytes for the value/payload of a key
    public int valueSize = 2;

    public double putProb = 0.8;
    public double putAsyncProb = 0.1;
    public double putAllProb = 0.1;

    private TestContext testContext;
    private HazelcastInstance hazelcastInstance;
    private byte[] value;
    private ICache<Object, Object> cache;
    private int configuredMaxSize;
    private Map<Integer, Object> putAllMap = new HashMap<Integer, Object>();

    // find estimated max size (entry count) that cache can reach at max
    private int estimatedMaxSize;

    private OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    @Setup
    public void setup(TestContext testContext) {
        this.testContext = testContext;
        hazelcastInstance = this.testContext.getTargetInstance();
        partitionCount = hazelcastInstance.getPartitionService().getPartitions().size();

        value = new byte[valueSize];
        Random random = new Random();
        random.nextBytes(value);

        CacheManager cacheManager = createCacheManager(hazelcastInstance);
        cache = (ICache<Object, Object>) cacheManager.getCache(basename);

        CacheConfig config = cache.getConfiguration(CacheConfig.class);
        LOGGER.info(basename + ": " + cache.getName() + " config=" + config);

        configuredMaxSize = config.getEvictionConfig().getSize();

        // we are explicitly using a random key so that all participants of the test do not put keys 0...max
        // the size of putAllMap is not guarantied to be configuredMaxSize / 2 as keys are random
        for (int i = 0; i < configuredMaxSize / 2; i++) {
            putAllMap.put(random.nextInt(), value);
        }

        int maxEstimatedPartitionSize = com.hazelcast.cache.impl.maxsize.impl.EntryCountCacheMaxSizeChecker
                .calculateMaxPartitionSize(configuredMaxSize, partitionCount);
        estimatedMaxSize = maxEstimatedPartitionSize * partitionCount;

        operationSelectorBuilder
                .addOperation(Operation.PUT, putProb)
                .addOperation(Operation.PUT_ASYNC, putAsyncProb)
                .addOperation(Operation.PUT_ALL, putAllProb);
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(basename);
        for (int i = 0; i < threadCount; i++) {
            spawner.spawn(new WorkerThread());
        }
        spawner.awaitCompletion();
    }

    private class WorkerThread implements Runnable {
        private final OperationSelector<Operation> selector = operationSelectorBuilder.build();
        private final Random random = new Random();
        private final Counter counter = new Counter();

        private int max = 0;

        @Override
        public void run() {
            while (!testContext.isStopped()) {

                int key = random.nextInt();

                switch (selector.select()) {
                    case PUT:
                        cache.put(key, value);
                        counter.put++;
                        break;

                    case PUT_ASYNC:
                        cache.putAsync(key, value);
                        counter.putAsync++;
                        break;

                    case PUT_ALL:
                        cache.putAll(putAllMap);
                        counter.putAll++;
                        break;

                    default:
                        throw new UnsupportedOperationException();
                }

                int size = cache.size();
                if (size > max) {
                    max = size;
                }

                if (size > estimatedMaxSize) {
                    fail(basename + ": cache " + cache.getName() + " size=" + cache.size()
                            + " configuredMaxSize=" + configuredMaxSize + " estimatedMaxSize=" + estimatedMaxSize);
                }
            }
            hazelcastInstance.getList(basename + "max").add(max);
            hazelcastInstance.getList(basename + "counter").add(counter);
        }

    }

    @Verify(global = true)
    public void globalVerify() {
        IList<Integer> results = hazelcastInstance.getList(basename + "max");
        int observedMaxSize = 0;
        for (int m : results) {
            if (observedMaxSize < m) {
                observedMaxSize = m;
            }
        }
        LOGGER.info(basename + ": cache " + cache.getName() + " size=" + cache.size() + " configuredMaxSize=" + configuredMaxSize
                + " observedMaxSize=" + observedMaxSize + " estimatedMaxSize=" + estimatedMaxSize);

        IList<Counter> counters = hazelcastInstance.getList(basename + "counter");
        Counter total = new Counter();
        for (Counter c : counters) {
            total.add(c);
        }
        LOGGER.info(basename + ": " + total);
        LOGGER.info(basename + ": putAllMap size=" + putAllMap.size());
    }

    public static class Counter implements Serializable {
        public int put = 0;
        public int putAsync = 0;
        public int putAll = 0;

        public void add(Counter c) {
            put += c.put;
            putAsync += c.putAsync;
            putAll += c.putAll;
        }

        @Override
        public String toString() {
            return "Counter{"
                    + "put=" + put
                    + ", putAsync=" + putAsync
                    + ", putAll=" + putAll
                    + '}';
        }
    }
}
