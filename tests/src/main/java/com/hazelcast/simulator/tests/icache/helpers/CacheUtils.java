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
