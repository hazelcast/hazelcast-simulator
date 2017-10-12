/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.icache.helpers.ICacheCreateDestroyCounter;

import javax.cache.Cache;
import javax.cache.CacheManager;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;

/**
 * Concurrently creates, deletes, destroys and puts data to an {@link com.hazelcast.cache.ICache}.
 *
 * This test is a subset of {@link MangleICacheTest}, so could be deleted.
 */
public class CreateDestroyICacheTest extends AbstractTest {

    public int keyCount = 100000;

    private IList<ICacheCreateDestroyCounter> counters;
    private CacheManager cacheManager;

    @Setup
    public void setup() {
        counters = targetInstance.getList(name);

        cacheManager = createCacheManager(targetInstance);
    }

    @TimeStep(prob = 0.4)
    public void createCache(ThreadState state) {
        try {
            cacheManager.getCache(name);
            state.counter.create++;
        } catch (IllegalStateException e) {
            state.counter.createException++;
        }
    }

    @TimeStep(prob = 0.2)
    public void putCache(ThreadState state) {
        try {
            Cache<Integer, Integer> cache = cacheManager.getCache(name);
            if (cache != null) {
                cache.put(state.randomInt(keyCount), state.randomInt());
                state.counter.put++;
            }
        } catch (IllegalStateException e) {
            state.counter.putException++;
        }
    }

    @TimeStep(prob = 0.2)
    public void closeCache(ThreadState state) {
        try {
            Cache cache = cacheManager.getCache(name);
            if (cache != null) {
                cache.close();
                state.counter.close++;
            }
        } catch (IllegalStateException e) {
            state.counter.closeException++;
        }
    }

    @TimeStep(prob = 0.2)
    public void destroyCache(ThreadState state) {
        try {
            cacheManager.destroyCache(name);
            state.counter.destroy++;
        } catch (IllegalStateException e) {
            state.counter.destroyException++;
        }
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        counters.add(state.counter);
    }

    public final class ThreadState extends BaseThreadState {

        private final ICacheCreateDestroyCounter counter = new ICacheCreateDestroyCounter();
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
