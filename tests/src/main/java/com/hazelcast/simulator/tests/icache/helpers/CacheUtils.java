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
package com.hazelcast.simulator.tests.icache.helpers;

import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.core.HazelcastInstance;

import javax.cache.CacheManager;
import javax.cache.spi.CachingProvider;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;

public final class CacheUtils {

    private CacheUtils() {
    }

    public static CacheManager createCacheManager(HazelcastInstance hazelcastInstance) {
        if (isMemberNode(hazelcastInstance)) {
            return createCacheManager(hazelcastInstance, new HazelcastServerCachingProvider());
        } else {
            return createCacheManager(hazelcastInstance, new HazelcastClientCachingProvider());
        }
    }

    public static CacheManager createCacheManager(HazelcastInstance hazelcastInstance, CachingProvider cachingProvider) {
        if (isMemberNode(hazelcastInstance)) {
            return createCacheManager(hazelcastInstance, (HazelcastServerCachingProvider) cachingProvider);
        } else {
            return createCacheManager(hazelcastInstance, (HazelcastClientCachingProvider) cachingProvider);
        }
    }

    static HazelcastServerCacheManager createCacheManager(HazelcastInstance instance, HazelcastServerCachingProvider hcp) {
        if (hcp == null) {
            hcp = new HazelcastServerCachingProvider();
        }
        return new HazelcastServerCacheManager(hcp, instance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
    }

    static HazelcastClientCacheManager createCacheManager(HazelcastInstance instance, HazelcastClientCachingProvider hcp) {
        if (hcp == null) {
            hcp = new HazelcastClientCachingProvider();
        }
        return new HazelcastClientCacheManager(hcp, instance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
    }
}
