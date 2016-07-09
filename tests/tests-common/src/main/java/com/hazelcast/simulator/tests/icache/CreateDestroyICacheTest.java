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
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.tests.icache.helpers.ICacheCreateDestroyCounter;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import javax.cache.Cache;
import javax.cache.CacheManager;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;

/**
 * In this test we are concurrently creating, deleting, destroying and putting to a cache.
 * However this test is a sub set of {@link MangleICacheTest}, so could be deleted.
 */
public class CreateDestroyICacheTest extends AbstractTest {

    private enum Operation {
        CREATE_CACHE,
        PUT_CACHE,
        CLOSE_CACHE,
        DESTROY_CACHE
    }

    public int keyCount = 100000;
    public double createCacheProb = 0.4;
    public double putCacheProb = 0.2;
    public double closeCacheProb = 0.2;
    public double destroyCacheProb = 0.2;

    private final OperationSelectorBuilder<Operation> builder = new OperationSelectorBuilder<Operation>();

    private IList<ICacheCreateDestroyCounter> counters;
    private CacheManager cacheManager;

    @Setup
    public void setup() {
        counters = targetInstance.getList(name);

        cacheManager = createCacheManager(targetInstance);

        builder.addOperation(Operation.CREATE_CACHE, createCacheProb)
                .addOperation(Operation.PUT_CACHE, putCacheProb)
                .addOperation(Operation.CLOSE_CACHE, closeCacheProb)
                .addOperation(Operation.DESTROY_CACHE, destroyCacheProb);
    }

    @RunWithWorker
    public Worker run() {
        return new Worker();
    }

    private final class Worker extends AbstractWorker<Operation> {

        private final ICacheCreateDestroyCounter counter = new ICacheCreateDestroyCounter();

        private Worker() {
            super(builder);
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            switch (operation) {
                case CREATE_CACHE:
                    try {
                        cacheManager.getCache(name);
                        counter.create++;
                    } catch (IllegalStateException e) {
                        counter.createException++;
                    }
                    break;

                case PUT_CACHE:
                    try {
                        Cache<Integer, Integer> cache = cacheManager.getCache(name);
                        if (cache != null) {
                            cache.put(randomInt(keyCount), randomInt());
                            counter.put++;
                        }
                    } catch (IllegalStateException e) {
                        counter.putException++;
                    }
                    break;

                case CLOSE_CACHE:
                    try {
                        Cache cache = cacheManager.getCache(name);
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
                        cacheManager.destroyCache(name);
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
        public void afterRun() {
            counters.add(counter);
        }
    }

    @Verify
    public void globalVerify() {
        ICacheCreateDestroyCounter total = new ICacheCreateDestroyCounter();
        for (ICacheCreateDestroyCounter counter : counters) {
            total.add(counter);
        }
        logger.info(name + ": " + total + " from " + counters.size() + " worker threads");
    }
}
