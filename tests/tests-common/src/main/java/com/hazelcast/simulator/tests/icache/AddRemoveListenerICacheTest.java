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

import com.hazelcast.core.IList;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.icache.helpers.ICacheEntryEventFilter;
import com.hazelcast.simulator.tests.icache.helpers.ICacheEntryListener;
import com.hazelcast.simulator.tests.icache.helpers.ICacheListenerOperationCounter;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;

/**
 * In this test we concurrently add remove cache listeners while putting and getting from the cache.
 *
 * This test is out side of normal usage, however has found problems where put operations hang.
 * This type of test could uncover memory leaks in the process of adding and removing listeners.
 * The max size of the cache used in this test is keyCount int key/value pairs.
 */
public class AddRemoveListenerICacheTest extends AbstractTest {

    public int keyCount = 1000;
    public boolean syncEvents = true;

    private final ICacheEntryListener<Integer, Long> listener = new ICacheEntryListener<Integer, Long>();
    private final ICacheEntryEventFilter<Integer, Long> filter = new ICacheEntryEventFilter<Integer, Long>();
    private IList<ICacheListenerOperationCounter> results;
    private CacheManager cacheManager;
    private Cache<Integer, Long> cache;
    private MutableCacheEntryListenerConfiguration<Integer, Long> listenerConfiguration;

    @Setup
    public void setup() {
        results = targetInstance.getList(name);
        cacheManager = createCacheManager(targetInstance);
        cacheManager.getCache(name);
    }

    @Prepare(global = false)
    public void prepare() {
        cache = cacheManager.getCache(name);

        listenerConfiguration = new MutableCacheEntryListenerConfiguration<Integer, Long>(
                FactoryBuilder.factoryOf(listener),
                FactoryBuilder.factoryOf(filter),
                false, syncEvents);
    }

    @TimeStep(prob = 0.25)
    public void register(ThreadState state) {
        try {
            cache.registerCacheEntryListener(listenerConfiguration);
            state.operationCounter.register++;
        } catch (IllegalArgumentException e) {
            state.operationCounter.registerIllegalArgException++;
        }
    }

    @TimeStep(prob = 0.25)
    public void deregister(ThreadState state) {
        cache.deregisterCacheEntryListener(listenerConfiguration);
        state.operationCounter.deRegister++;
    }

    @TimeStep(prob = 0.25)
    public void put(ThreadState state) {
        cache.put(state.randomInt(keyCount), 1L);
        state.operationCounter.put++;
    }

    @TimeStep(prob = 0.25)
    public void get(ThreadState state) {
        cache.get(state.randomInt(keyCount));
        state.operationCounter.put++;
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        logger.info(name + ": " + state.operationCounter);
        results.add(state.operationCounter);
    }

    public class ThreadState extends BaseThreadState {

        private final ICacheListenerOperationCounter operationCounter = new ICacheListenerOperationCounter();
    }

    @Verify(global = true)
    public void globalVerify() {
        ICacheListenerOperationCounter total = new ICacheListenerOperationCounter();
        for (ICacheListenerOperationCounter i : results) {
            total.add(i);
        }
        logger.info(name + ": " + total + " from " + results.size() + " worker threads");
    }
}
