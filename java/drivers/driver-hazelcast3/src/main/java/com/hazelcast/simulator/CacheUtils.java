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
package com.hazelcast.simulator;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.cache.ICache;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceProxy;
import org.apache.log4j.Logger;

import javax.cache.Cache;
import javax.cache.Caching;
import javax.cache.expiry.Duration;
import javax.cache.spi.CachingProvider;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.HazelcastCachingProvider.propertiesByInstanceName;
import static com.hazelcast.simulator.utils.CommonUtils.sleepTimeUnit;
import static java.lang.String.format;

public final class CacheUtils {

    private CacheUtils() {
    }

    public static void sleepDurationTwice(Logger logger, Duration duration) {
        if (duration.isEternal() || duration.isZero()) {
            return;
        }

        TimeUnit timeUnit = duration.getTimeUnit();
        long timeout = duration.getDurationAmount() * 2;
        logger.info(format("Sleeping for %d %s...", timeout, timeUnit));
        sleepTimeUnit(timeUnit, timeout);
    }

    public static <K, V> ICache<K, V> getCache(HazelcastInstance hazelcastInstance, String cacheName) {
        HazelcastCacheManager cacheManager = createCacheManager(hazelcastInstance);
        return getCache(cacheManager, cacheName);
    }

    @SuppressWarnings("unchecked")
    public static <K, V> ICache<K, V> getCache(HazelcastCacheManager cacheManager, String cacheName) {
        Cache<Object, Object> cache = cacheManager.getCache(cacheName);
        return cache.unwrap(ICache.class);
    }

    /**
     * Obtain the default CacheManager
     *
     * @param hazelcastInstance the HazelcastInstance
     * @return a CacheManager for the default URI and ClassLoader running on the provided HazelcastInstance
     */
    public static HazelcastCacheManager createCacheManager(HazelcastInstance hazelcastInstance) {
        return createCacheManager(hazelcastInstance, null);
    }

    /**
     * Creates a cache manager
     *
     * @param hazelcastInstance the HazelcastInstance
     * @param uri               the uri
     * @return the CacheManager for given URI and default ClassLoader.
     */
    public static HazelcastCacheManager createCacheManager(HazelcastInstance hazelcastInstance, URI uri) {
        Properties properties = propertiesByInstanceName(hazelcastInstance.getName());
        CachingProvider cachingProvider = getCachingProvider(hazelcastInstance);
        return (HazelcastCacheManager) cachingProvider.getCacheManager(uri, null, properties);
    }

    public static CachingProvider getCachingProvider(HazelcastInstance hazelcastInstance) {
        if (isMemberNode(hazelcastInstance)) {
            return Caching.getCachingProvider("com.hazelcast.cache.impl.HazelcastServerCachingProvider");
        } else {
            return Caching.getCachingProvider("com.hazelcast.client.cache.impl.HazelcastClientCachingProvider");
        }
    }

    public static boolean isMemberNode(HazelcastInstance instance) {
        return instance instanceof HazelcastInstanceProxy;
    }
}
