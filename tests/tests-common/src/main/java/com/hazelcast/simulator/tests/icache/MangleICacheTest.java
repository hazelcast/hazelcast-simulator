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
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.icache.helpers.CacheUtils;
import com.hazelcast.simulator.tests.icache.helpers.ICacheOperationCounter;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import java.net.URI;
import java.net.URISyntaxException;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.rethrow;

/**
 * In this tests we are intentionally creating, destroying, closing and using cache managers and their caches.
 *
 * This type of cache usage is well outside normal usage, however we found several bugs with this test. It could highlight memory
 * leaks when repeatedly creating and destroying caches and/or managers, something that regular test would not find.
 */
public class MangleICacheTest extends AbstractTest {

    // properties
    public int maxCaches = 100;
    public int keyCount = 100000;
    // used to randomize cache manager names
    public int cacheManagerMaxSuffix = 1000;

    private IList<ICacheOperationCounter> results;

    @Setup
    public void setup() {
        results = targetInstance.getList(name);
    }

    @BeforeRun
    public void beforeRun(ThreadState state) {
        state.createNewCacheManager();
    }

    @TimeStep(prob = 0.1)
    public void createCacheManager(ThreadState state) {
        try {
            state.createNewCacheManager();
            state.counter.createCacheManager++;
        } catch (CacheException e) {
            state.counter.createCacheManagerException++;
        }
    }

    @TimeStep(prob = 0.1)
    public void cacheManagerClose(ThreadState state) {
        try {
            state.cacheManager.close();
            state.counter.cacheManagerClose++;
        } catch (CacheException e) {
            state.counter.cacheManagerCloseException++;
        }
    }

    @TimeStep(prob = 0.1)
    public void createCache(ThreadState state) {
        try {
            int cacheNumber = state.randomInt(maxCaches);
            state.cacheManager.getCache(name + cacheNumber);
            state.counter.create++;
        } catch (CacheException e) {
            state.counter.createException++;
        } catch (IllegalStateException e) {
            state.counter.createException++;
        }
    }

    @TimeStep(prob = 0.1)
    public void closeCache(ThreadState state) {
        int cacheNumber = state.randomInt(maxCaches);
        Cache cache = state.getCacheIfExists(cacheNumber);
        try {
            if (cache != null) {
                cache.close();
                state.counter.cacheClose++;
            }
        } catch (CacheException e) {
            state.counter.cacheCloseException++;
        } catch (IllegalStateException e) {
            state.counter.cacheCloseException++;
        }
    }

    @TimeStep(prob = 0.2)
    public void destroyCache(ThreadState state) {
        try {
            int cacheNumber = state.randomInt(maxCaches);
            state.cacheManager.destroyCache(name + cacheNumber);
            state.counter.destroy++;
        } catch (CacheException e) {
            state.counter.destroyException++;
        } catch (IllegalStateException e) {
            state.counter.destroyException++;
        }
    }

    @TimeStep(prob = 0.4)
    public void putCache(ThreadState state) {
        int cacheNumber = state.randomInt(maxCaches);
        Cache<Integer, Integer> cache = state.getCacheIfExists(cacheNumber);
        try {
            if (cache != null) {
                cache.put(state.randomInt(keyCount), state.randomInt());
                state.counter.put++;
            }
        } catch (CacheException e) {
            state.counter.getPutException++;
        } catch (IllegalStateException e) {
            state.counter.getPutException++;
        }
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        results.add(state.counter);
    }

    public class ThreadState extends BaseThreadState {

        private final ICacheOperationCounter counter = new ICacheOperationCounter();
        private CacheManager cacheManager;

        private void createNewCacheManager() {
            if (cacheManager != null) {
                cacheManager.close();
            }
            try {
                URI uri = new URI(name + randomInt(cacheManagerMaxSuffix));
                cacheManager = CacheUtils.createCacheManager(targetInstance, uri);
            } catch (URISyntaxException e) {
                throw rethrow(e);
            }
        }

        private Cache<Integer, Integer> getCacheIfExists(int cacheNumber) {
            try {
                Cache<Integer, Integer> cache = cacheManager.getCache(name + cacheNumber);
                counter.getCache++;
                return cache;

            } catch (CacheException e) {
                counter.getCacheException++;

            } catch (IllegalStateException e) {
                counter.getCacheException++;
            }
            return null;
        }
    }

    @Verify
    public void globalVerify() {
        ICacheOperationCounter total = new ICacheOperationCounter();
        for (ICacheOperationCounter counter : results) {
            total.add(counter);
        }
        logger.info(name + ": " + total + " from " + results.size() + " worker threads");
    }
}
