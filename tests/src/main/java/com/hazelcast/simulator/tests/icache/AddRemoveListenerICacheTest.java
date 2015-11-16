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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.icache.helpers.ICacheEntryEventFilter;
import com.hazelcast.simulator.tests.icache.helpers.ICacheEntryListener;
import com.hazelcast.simulator.tests.icache.helpers.ICacheListenerOperationCounter;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

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
public class AddRemoveListenerICacheTest {

    private enum Operation {
        REGISTER,
        DE_REGISTER,
        PUT,
        GET
    }

    private static final ILogger LOGGER = Logger.getLogger(AddRemoveListenerICacheTest.class);

    public String basename = AddRemoveListenerICacheTest.class.getSimpleName();
    public int keyCount = 1000;
    public boolean syncEvents = true;

    public double registerProb = 0.25;
    public double deRegisterProb = 0.25;
    public double putProb = 0.25;
    public double getProb = 0.25;

    private final OperationSelectorBuilder<Operation> builder = new OperationSelectorBuilder<Operation>();
    private final ICacheEntryListener<Integer, Long> listener = new ICacheEntryListener<Integer, Long>();
    private final ICacheEntryEventFilter<Integer, Long> filter = new ICacheEntryEventFilter<Integer, Long>();

    private IList<ICacheListenerOperationCounter> results;
    private CacheManager cacheManager;
    private Cache<Integer, Long> cache;
    private MutableCacheEntryListenerConfiguration<Integer, Long> listenerConfiguration;

    @Setup
    public void setup(TestContext testContext) {
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
        results = hazelcastInstance.getList(basename);

        cacheManager = createCacheManager(hazelcastInstance);
        cacheManager.getCache(basename);

        builder.addOperation(Operation.REGISTER, registerProb)
                .addOperation(Operation.DE_REGISTER, deRegisterProb)
                .addOperation(Operation.PUT, putProb)
                .addOperation(Operation.GET, getProb);
    }

    @Warmup(global = false)
    public void warmup() {
        cache = cacheManager.getCache(basename);

        listenerConfiguration = new MutableCacheEntryListenerConfiguration<Integer, Long>(
                FactoryBuilder.factoryOf(listener),
                FactoryBuilder.factoryOf(filter),
                false, syncEvents);
    }

    @Verify(global = true)
    public void globalVerify() {
        ICacheListenerOperationCounter total = new ICacheListenerOperationCounter();
        for (ICacheListenerOperationCounter i : results) {
            total.add(i);
        }
        LOGGER.info(basename + ": " + total + " from " + results.size() + " worker threads");
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        private final ICacheListenerOperationCounter operationCounter = new ICacheListenerOperationCounter();

        public Worker() {
            super(builder);
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            switch (operation) {
                case REGISTER:
                    try {
                        cache.registerCacheEntryListener(listenerConfiguration);
                        operationCounter.register++;
                    } catch (IllegalArgumentException e) {
                        operationCounter.registerIllegalArgException++;
                    }
                    break;
                case DE_REGISTER:
                    cache.deregisterCacheEntryListener(listenerConfiguration);
                    operationCounter.deRegister++;
                    break;
                case PUT:
                    cache.put(randomInt(keyCount), 1L);
                    operationCounter.put++;
                    break;
                case GET:
                    cache.get(randomInt(keyCount));
                    operationCounter.put++;
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        protected void afterRun() {
            LOGGER.info(basename + ": " + operationCounter);
            results.add(operationCounter);
        }
    }

    public static void main(String[] args) throws Exception {
        new TestRunner<AddRemoveListenerICacheTest>(new AddRemoveListenerICacheTest()).run();
    }
}
