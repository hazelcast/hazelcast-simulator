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
import com.hazelcast.simulator.test.BaseThreadContext;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.AbstractTest;
import com.hazelcast.simulator.tests.icache.helpers.ICacheCreateDestroyCounter;

import javax.cache.Cache;
import javax.cache.CacheManager;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;

/**
 * In this test we are concurrently creating, deleting, destroying and putting to a cache.
 * However this test is a sub set of {@link MangleICacheTest}, so could be deleted.
 */
public class CreateDestroyICacheTest extends AbstractTest {

    public int keyCount = 100000;

    private IList<ICacheCreateDestroyCounter> counters;
    private CacheManager cacheManager;

    @Setup
    public void setup() {
        counters = targetInstance.getList(basename);
        cacheManager = createCacheManager(targetInstance);
    }

    @TimeStep(prob = 0.4)
    public void createCatch(ThreadContext context) {
        try {
            cacheManager.getCache(basename);
            context.counter.create++;
        } catch (IllegalStateException e) {
            context.counter.createException++;
        }
    }

    @TimeStep(prob = 0.3)
    public void putCache(ThreadContext context) {
        try {
            Cache<Integer, Integer> cache = cacheManager.getCache(basename);
            if (cache != null) {
                cache.put(context.randomInt(keyCount), context.randomInt());
                context.counter.put++;
            }
        } catch (IllegalStateException e) {
            context.counter.putException++;
        }
    }

    @TimeStep(prob = 0.2)
    public void closeCache(ThreadContext context) {
        try {
            Cache cache = cacheManager.getCache(basename);
            if (cache != null) {
                cache.close();
                context.counter.close++;
            }
        } catch (IllegalStateException e) {
            context.counter.closeException++;
        }
    }

    @TimeStep(prob = 0.2)
    public void destroyCache(ThreadContext context) {
        try {
            cacheManager.destroyCache(basename);
            context.counter.destroy++;
        } catch (IllegalStateException e) {
            context.counter.destroyException++;
        }
    }

    @AfterRun
    public void afterRun(ThreadContext context) {
        counters.add(context.counter);
    }

    public final class ThreadContext extends BaseThreadContext {
        private final ICacheCreateDestroyCounter counter = new ICacheCreateDestroyCounter();
    }

    @Verify
    public void globalVerify() {
        ICacheCreateDestroyCounter total = new ICacheCreateDestroyCounter();
        for (ICacheCreateDestroyCounter counter : counters) {
            total.add(counter);
        }
        logger.info(basename + ": " + total + " from " + counters.size() + " worker threads");
    }
}
