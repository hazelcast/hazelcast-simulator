package com.hazelcast.simulator.tests.icache.helpers;

import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
import com.hazelcast.core.HazelcastInstance;

import javax.cache.CacheManager;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;

public final class CacheUtils {

    private CacheUtils() {
    }

    public static CacheManager getCacheManager(HazelcastInstance hazelcastInstance) {
        if (isMemberNode(hazelcastInstance)) {
            HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
            return new HazelcastServerCacheManager(
                    hcp, hazelcastInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        } else {
            HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
            return new HazelcastClientCacheManager(
                    hcp, hazelcastInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        }
    }
}
