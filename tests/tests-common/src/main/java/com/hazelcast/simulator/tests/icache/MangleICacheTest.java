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
import com.hazelcast.simulator.tests.icache.helpers.CacheUtils;
import com.hazelcast.simulator.tests.icache.helpers.ICacheOperationCounter;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

/**
 * In this tests we are intentionally creating, destroying, closing and using cache managers and their caches.
 * <p>
 * This type of cache usage is well outside normal usage, however we found several bugs with this test. It could highlight memory
 * leaks when repeatedly creating and destroying caches and/or managers, something that regular test would not find.
 */
public class MangleICacheTest extends AbstractTest {

    public int maxCaches = 100;

    public int keyCount = 100000;
    public double createCacheManagerProb = 0.1;
    public double cacheManagerCloseProb = 0.1;
    public double cachingProviderCloseProb = 0.1;
    public double createCacheProb = 0.1;
    public double destroyCacheProb = 0.2;
    public double putCacheProb = 0.3;
    public double closeCacheProb = 0.1;

    private IList<ICacheOperationCounter> results;

    @Setup
    public void setup() {
        results = targetInstance.getList(name);
    }

    @TimeStep
    public void closeCachingProvider(ThreadContext context) {
        try {
            CachingProvider provider = context.cacheManager.getCachingProvider();
            if (provider != null) {
                provider.close();
                context.counter.cachingProviderClose++;
            }
        } catch (CacheException e) {
            context.counter.cachingProviderCloseException++;
        }
    }

    @TimeStep
    public void createCacheManager(ThreadContext context) {
        try {
            context.createNewCacheManager();
            context.counter.createCacheManager++;
        } catch (CacheException e) {
            context.counter.createCacheManagerException++;
        }
    }

    @TimeStep
    public void closeCacheManager(ThreadContext context) {
        try {
            context.cacheManager.close();
            context.counter.cacheManagerClose++;
        } catch (CacheException e) {
            context.counter.cacheManagerCloseException++;
        }
    }

    @TimeStep
    public void getCache(ThreadContext context) {
        try {
            int cacheNumber = context.randomInt(maxCaches);
            context.cacheManager.getCache(name + cacheNumber);
            context.counter.create++;
        } catch (CacheException e) {
            context.counter.createException++;
        } catch (IllegalStateException e) {
            context.counter.createException++;
        }
    }

    @TimeStep
    public void closeCache(ThreadContext context) {
        int cacheNumber = context.randomInt(maxCaches);
        Cache cache = context.getCacheIfExists(cacheNumber);
        try {
            if (cache != null) {
                cache.close();
                context.counter.cacheClose++;
            }
        } catch (CacheException e) {
            context.counter.cacheCloseException++;
        } catch (IllegalStateException e) {
            context.counter.cacheCloseException++;
        }
    }

    @TimeStep
    public void destroyCache(ThreadContext context) {
        try {
            int cacheNumber = context.randomInt(maxCaches);
            context.cacheManager.destroyCache(name + cacheNumber);
            context.counter.destroy++;
        } catch (CacheException e) {
            context.counter.destroyException++;
        } catch (IllegalStateException e) {
            context.counter.destroyException++;
        }
    }

    @TimeStep
    public void put(ThreadContext context) {
        int cacheNumber = context.randomInt(maxCaches);
        Cache<Integer, Integer> cache = context.getCacheIfExists(cacheNumber);
        try {
            if (cache != null) {
                cache.put(context.randomInt(keyCount), context.randomInt());
                context.counter.put++;
            }
        } catch (CacheException e) {
            context.counter.getPutException++;
        } catch (IllegalStateException e) {
            context.counter.getPutException++;
        }
    }

    @AfterRun
    public void afterRun(ThreadContext context) {
        results.add(context.counter);
    }

    public class ThreadContext extends BaseThreadContext {

        private final ICacheOperationCounter counter = new ICacheOperationCounter();

        private CacheManager cacheManager;

        public ThreadContext() {
            createNewCacheManager();
        }

        private void createNewCacheManager() {
            CachingProvider currentCachingProvider = null;
            if (cacheManager != null) {
                currentCachingProvider = cacheManager.getCachingProvider();
                cacheManager.close();
            }
            cacheManager = CacheUtils.createCacheManager(targetInstance, currentCachingProvider);
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
