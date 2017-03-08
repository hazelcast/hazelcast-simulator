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
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

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
public class EvictionICacheTest extends AbstractTest {

    private static final double TOLERANCE_FACTOR = 1.5;
    public int partitionCount;

    // number of bytes for the value/payload of a key
    public int valueSize = 2;

    private byte[] value;
    private ICache<Object, Object> cache;
    private int configuredMaxSize;
    private Map<Integer, Object> putAllMap = new HashMap<Integer, Object>();

    // find estimated max size (entry count) that cache can reach at max
    private int estimatedMaxSize;

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

        estimatedMaxSize = (int) (configuredMaxSize * TOLERANCE_FACTOR);
    }

    @TimeStep(prob = 0.8)
    public void put(ThreadState state) {
        int key = state.randomInt();
        cache.put(key, value);
        state.counter.put++;
        state.assertSize();
    }

    @TimeStep(prob = 0.1)
    public void putAsync(ThreadState state) {
        int key = state.randomInt();
        cache.putAsync(key, value);
        state.counter.putAsync++;
        state.assertSize();
    }

    @TimeStep(prob = 0.1)
    public void putAll(ThreadState state) {
        cache.putAll(putAllMap);
        state.counter.putAll++;
        state.assertSize();
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        targetInstance.getList(name + "max").add(state.max);
        targetInstance.getList(name + "counter").add(state.counter);
    }

    public class ThreadState extends BaseThreadState {
        final Counter counter = new Counter();
        int max = 0;

        void assertSize() {
            int size = cache.size();
            if (size > max) {
                max = size;
            }

            if (size > estimatedMaxSize) {
                fail(name + ": cache " + cache.getName() + " size=" + cache.size()
                        + " configuredMaxSize=" + configuredMaxSize + " estimatedMaxSize=" + estimatedMaxSize);
            }
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
        int put = 0;
        int putAsync = 0;
        int putAll = 0;

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
