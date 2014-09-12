/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer.tests.icache;

import com.hazelcast.cache.impl.HazelcastCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.HazelcastClientCacheManager;
import com.hazelcast.client.cache.HazelcastClientCachingProvider;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import javax.cache.CacheException;
import java.io.Serializable;

import static junit.framework.Assert.assertEquals;

public class ConcurentCreateICacheTest {

    private final static ILogger log = Logger.getLogger(ConcurentCreateICacheTest.class);

    public int memberCount=-1;
    public int clientCount=-1;

    private HazelcastInstance targetInstance;
    private HazelcastCacheManager cacheManager;
    private String baseName;
    private Counter counter = new Counter();

    @Setup
    public void setup(TestContext testContext) throws Exception {

        if(memberCount==-1 || clientCount==-1){
            throw new IllegalStateException("must Set memberCount and clientCount properties");
        }

        targetInstance = testContext.getTargetInstance();
        baseName = testContext.getTestId();

        if (TestUtils.isMemberNode(targetInstance)) {
            HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
            cacheManager = new HazelcastServerCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        } else {
            HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
            cacheManager = new HazelcastClientCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        }

        final CacheConfig config = new CacheConfig();

        config.setName(baseName);
        config.setTypes(Object.class, Object.class);

        try {
            cacheManager.createCache(baseName, config);
            counter.create++;
        } catch (CacheException e) {
            log.severe(baseName +": createCache exception "+e, e);
            counter.createException++;
        }

        targetInstance.getList(baseName).add(counter);
    }

    @Run
    public void run(){
    }

    @Verify(global = true)
    public void verify() throws Exception {
        IList<Counter> counters = targetInstance.getList(baseName);
        Counter total = new Counter();
        for(Counter c : counters){
            total.add(c);
        }
        log.info(baseName + ": " + total + " from " + counters.size() + " worker threads");

        assertEquals("multipule thread/Members/clients created the ", 1,total.create);
        int expectedExceptions=memberCount+clientCount-1;
        assertEquals("multi thread/Members/clients createCache "+baseName, expectedExceptions, total.createException);
    }

    static class Counter implements Serializable {

        public long create = 0;
        public long createException = 0;

        public void add(Counter c) {
            create += c.create;
            createException += c.createException;
        }

        @Override
        public String toString() {
            return "Counter{" +
                    " create=" + create +
                    ", createException=" + createException +
                    '}';
        }
    }
}