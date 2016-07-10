/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IList;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import javax.cache.CacheManager;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static com.hazelcast.simulator.tests.icache.EvictionICacheTest.Operation.PUT;
import static com.hazelcast.simulator.tests.icache.EvictionICacheTest.Operation.PUT_ALL;
import static com.hazelcast.simulator.tests.icache.EvictionICacheTest.Operation.PUT_ASYNC;
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
public class EvictionICacheTest extends AbstractTest {

    enum Operation {
        PUT,
        PUT_ASYNC,
        PUT_ALL,
    }

    public int threadCount = 3;
    public int partitionCount;

    // number of bytes for the value/payload of a key
    public int valueSize = 2;

    public double putProb = 0.8;
    public double putAsyncProb = 0.1;
    public double putAllProb = 0.1;

    private byte[] value;
    private ICache<Object, Object> cache;
    private int configuredMaxSize;
    private Map<Integer, Object> putAllMap = new HashMap<Integer, Object>();

    // find estimated max size (entry count) that cache can reach at max
    private int estimatedMaxSize;

    private OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    @Setup
    public void setup() {
        partitionCount = targetInstance.getPartitionService().getPartitions().size();

        value = new byte[valueSize];
        Random random = new Random();
        random.nextBytes(value);

        CacheManager cacheManager = createCacheManager(targetInstance);
        cache = (ICache<Object, Object>) cacheManager.getCache(name);

        CacheConfig config = cache.getConfiguration(CacheConfig.class);
        logger.info(name + ": " + cache.getName() + " config=" + config);

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
                .addOperation(PUT, putProb)
                .addOperation(PUT_ASYNC, putAsyncProb)
                .addOperation(PUT_ALL, putAllProb);
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {
        private final Counter counter = new Counter();
        private int max = 0;

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            int key = randomInt();
            switch (operation) {
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
                fail(name + ": cache " + cache.getName() + " size=" + cache.size()
                        + " configuredMaxSize=" + configuredMaxSize + " estimatedMaxSize=" + estimatedMaxSize);
            }
        }

        @Override
        public void afterRun() throws Exception {
            targetInstance.getList(name + "max").add(max);
            targetInstance.getList(name + "counter").add(counter);
        }
    }

    @Verify
    public void globalVerify() {
        IList<Integer> results = targetInstance.getList(name + "max");
        int observedMaxSize = 0;
        for (int m : results) {
            if (observedMaxSize < m) {
                observedMaxSize = m;
            }
        }
        logger.info(name + ": cache " + cache.getName() + " size=" + cache.size() + " configuredMaxSize=" + configuredMaxSize
                + " observedMaxSize=" + observedMaxSize + " estimatedMaxSize=" + estimatedMaxSize);

        IList<Counter> counters = targetInstance.getList(name + "counter");
        Counter total = new Counter();
        for (Counter c : counters) {
            total.add(c);
        }
        logger.info(name + ": " + total);
        logger.info(name + ": putAllMap size=" + putAllMap.size());
    }

    private static class Counter implements Serializable {
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
