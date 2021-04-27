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

import com.hazelcast.collection.IList;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.icache.helpers.ICacheReadWriteCounter;
import com.hazelcast.simulator.tests.icache.helpers.RecordingCacheLoader;
import com.hazelcast.simulator.tests.icache.helpers.RecordingCacheWriter;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.FactoryBuilder;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;
import static org.junit.Assert.assertNotNull;

/**
 * This tests concurrent load write and delete calls to CacheLoader. Via put, remove and get calls to a cache
 * we can configure a delay in the load write and delete.
 * A large delay and high concurrent calls to loadAll could overflow some internal queues.
 * We verify that the cache contains all keys and that the keys have been loaded through a loader instance.
 */
public class ReadWriteICacheTest extends HazelcastTest {

    public int keyCount = 10;
    public int putDelayMs = 0;
    public int getDelayMs = 0;
    public int removeDelayMs = 0;

    private IList<ICacheReadWriteCounter> counters;
    private CacheConfig<Integer, Integer> config;
    private Cache<Integer, Integer> cache;

    @Setup
    public void setup() {
        counters = targetInstance.getList(name + "counters");

        RecordingCacheLoader<Integer> loader = new RecordingCacheLoader<>();
        loader.loadDelayMs = getDelayMs;

        RecordingCacheWriter<Integer, Integer> writer = new RecordingCacheWriter<>();
        writer.writeDelayMs = putDelayMs;
        writer.deleteDelayMs = removeDelayMs;

        config = new CacheConfig<>();
        config.setReadThrough(true);
        config.setWriteThrough(true);
        config.setCacheLoaderFactory(FactoryBuilder.factoryOf(loader));
        config.setCacheWriterFactory(FactoryBuilder.factoryOf(writer));

        CacheManager cacheManager = createCacheManager(targetInstance);
        cacheManager.createCache(name, config);
        cache = cacheManager.getCache(name);
    }

    @TimeStep(prob = 0.4)
    public void put(ThreadState state) {
        int key = state.randomInt(keyCount);
        cache.put(key, key);
        state.counter.put++;
    }

    @TimeStep(prob = 0.4)
    public void get(ThreadState state) {
        int key = state.randomInt(keyCount);
        Object o = cache.get(key);
        assertNotNull(o);
        state.counter.get++;
    }

    @TimeStep(prob = 0.2)
    public void remove(ThreadState state) {
        int key = state.randomInt(keyCount);
        cache.remove(key);
        state.counter.remove++;
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        counters.add(state.counter);
    }

    public final class ThreadState extends BaseThreadState {

        private final ICacheReadWriteCounter counter = new ICacheReadWriteCounter();
    }

    @Verify(global = false)
    public void verify() {
        RecordingCacheLoader loader = (RecordingCacheLoader) config.getCacheLoaderFactory().create();
        RecordingCacheWriter writer = (RecordingCacheWriter) config.getCacheWriterFactory().create();

        logger.info(name + ": " + loader);
        logger.info(name + ": " + writer);
    }

    @Verify(global = true)
    public void globalVerify() {
        ICacheReadWriteCounter total = new ICacheReadWriteCounter();
        for (ICacheReadWriteCounter counter : counters) {
            total.add(counter);
        }
        logger.info(name + ": " + total + " from " + counters.size() + " worker threads");
    }
}
