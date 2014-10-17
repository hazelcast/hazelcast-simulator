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

import com.hazelcast.cache.ICache;
import com.hazelcast.cache.impl.HazelcastCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCacheManager;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import com.hazelcast.client.cache.impl.HazelcastClientCacheManager;
import com.hazelcast.client.cache.impl.HazelcastClientCachingProvider;
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
import java.util.Random;


/**
 * In This tests we are concurrently creating deleting destroying and putting to a cache.
 * However this test is a sub set of MangleIcacheTest ? so could be deleted
 */
public class CreateDestroyICacheTest {

    private final static ILogger log = Logger.getLogger(CreateDestroyICacheTest.class);

    public int threadCount = 3;
    public double createCacheProb=0.4;
    public double putCacheProb=0.2;
    public double closeCacheProb=0.2;
    public double destroyCacheProb=0.2;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private HazelcastCacheManager cacheManager;
    private String basename;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        basename = testContext.getTestId();

        if (TestUtils.isMemberNode(targetInstance)) {
            HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
            cacheManager = new HazelcastServerCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        } else {
            HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
            cacheManager = new HazelcastClientCacheManager(
                    hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();
        private final CacheConfig config = new CacheConfig();
        private final Counter counter = new Counter();

        public void run() {
            config.setName(basename);

            while (!testContext.isStopped()) {
                double chance = random.nextDouble();
                if ((chance -= createCacheProb) < 0) {
                    try {
                        cacheManager.createCache(basename, config);
                        counter.create++;
                    } catch (CacheException e) {
                        counter.createException++;
                    }
                } else if ((chance -= putCacheProb) < 0) {
                    try{
                        ICache cache = cacheManager.getCache(basename);
                        if(cache!=null){
                            cache.put(random.nextInt(), random.nextInt());
                            counter.put++;
                        }
                    } catch (IllegalStateException e){
                        counter.putException++;
                    }
                } else if ((chance -= closeCacheProb) < 0){
                    try{
                        ICache cache = cacheManager.getCache(basename);
                        if(cache!=null){
                            cache.close();
                            counter.close++;
                        }
                    } catch (IllegalStateException e){
                        counter.closeException++;
                    }
                } else if ((chance -= destroyCacheProb) < 0) {
                    try{
                        cacheManager.destroyCache(basename);
                        counter.destroy++;
                    } catch (IllegalStateException e){
                        counter.destroyException++;
                    }
                }
            }
            targetInstance.getList(basename).add(counter);
        }
    }

    @Verify(global = true)
    public void verify() throws Exception {
        IList<Counter> counters = targetInstance.getList(basename);
        Counter total = new Counter();
        for(Counter c : counters){
            total.add(c);
        }
        log.info(basename + ": " + total + " from " + counters.size() + " worker threads");
    }

    public static class Counter implements Serializable {

        public long put = 0;
        public long create = 0;
        public long close=0;
        public long destroy = 0;

        public long putException = 0;
        public long createException = 0;
        public long closeException = 0;
        public long destroyException = 0;

        public void add(Counter c) {
            put += c.put;
            create += c.create;
            close += c.close;
            destroy += c.destroy;

            putException += c.putException;
            createException += c.createException;
            closeException += c. closeException;
            destroyException += c.destroyException;
        }

        @Override
        public String toString() {
            return "Counter{" +
                    "put=" + put +
                    ", create=" + create +
                    ", close=" + close +
                    ", destroy=" + destroy +
                    ", putException=" + putException +
                    ", createException=" + createException +
                    ", closeException=" + closeException +
                    ", destroyException=" + destroyException +
                    '}';
        }
    }
}