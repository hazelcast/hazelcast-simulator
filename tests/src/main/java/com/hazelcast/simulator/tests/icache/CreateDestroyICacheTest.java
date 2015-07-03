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

import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.icache.helpers.ICacheCreateDestroyCounter;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.getCacheManager;

/**
 * In this test we are concurrently creating, deleting, destroying and putting to a cache.
 * However this test is a sub set of {@link MangleICacheTest}, so could be deleted.
 */
public class CreateDestroyICacheTest {

    private static final ILogger LOGGER = Logger.getLogger(CreateDestroyICacheTest.class);

    private enum Operation {
        CREATE_CACHE,
        PUT_CACHE,
        CLOSE_CACHE,
        DESTROY_CACHE
    }

    public double createCacheProb = 0.4;
    public double putCacheProb = 0.2;
    public double closeCacheProb = 0.2;
    public double destroyCacheProb = 0.2;

    private final OperationSelectorBuilder<Operation> builder = new OperationSelectorBuilder<Operation>();

    private HazelcastInstance hazelcastInstance;
    private CacheManager cacheManager;
    private String basename;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        hazelcastInstance = testContext.getTargetInstance();
        basename = testContext.getTestId();

        cacheManager = getCacheManager(hazelcastInstance);

        builder.addOperation(Operation.CREATE_CACHE, createCacheProb)
                .addOperation(Operation.PUT_CACHE, putCacheProb)
                .addOperation(Operation.CLOSE_CACHE, closeCacheProb)
                .addOperation(Operation.DESTROY_CACHE, destroyCacheProb);
    }

    @Verify(global = true)
    public void verify() throws Exception {
        IList<ICacheCreateDestroyCounter> counters = hazelcastInstance.getList(basename);
        ICacheCreateDestroyCounter total = new ICacheCreateDestroyCounter();
        for (ICacheCreateDestroyCounter c : counters) {
            total.add(c);
        }
        LOGGER.info(basename + ": " + total + " from " + counters.size() + " worker threads");
    }

    @RunWithWorker
    public Worker run() {
        return new Worker();
    }

    private final class Worker extends AbstractWorker<Operation> {

        private final ICacheCreateDestroyCounter counter = new ICacheCreateDestroyCounter();

        private final CacheConfig config;

        private Worker() {
            super(builder);

            config = new CacheConfig();
            config.setName(basename);
        }

        @Override
        protected void timeStep(Operation operation) {
            switch (operation) {
                case CREATE_CACHE:
                    try {
                        cacheManager.createCache(basename, config);
                        counter.create++;
                    } catch (CacheException e) {
                        counter.createException++;
                    }
                    break;

                case PUT_CACHE:
                    try {
                        Cache<Integer, Integer> cache = cacheManager.getCache(basename);
                        if (cache != null) {
                            cache.put(randomInt(), randomInt());
                            counter.put++;
                        }
                    } catch (IllegalStateException e) {
                        counter.putException++;
                    }
                    break;

                case CLOSE_CACHE:
                    try {
                        Cache cache = cacheManager.getCache(basename);
                        if (cache != null) {
                            cache.close();
                            counter.close++;
                        }
                    } catch (IllegalStateException e) {
                        counter.closeException++;
                    }
                    break;

                case DESTROY_CACHE:
                    try {
                        cacheManager.destroyCache(basename);
                        counter.destroy++;
                    } catch (IllegalStateException e) {
                        counter.destroyException++;
                    }
                    break;

                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        protected void afterRun() {
            hazelcastInstance.getList(basename).add(counter);
        }
    }
}
