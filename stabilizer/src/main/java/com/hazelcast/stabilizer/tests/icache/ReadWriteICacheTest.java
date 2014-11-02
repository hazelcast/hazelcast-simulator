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

import javax.cache.CacheManager;;
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
import com.hazelcast.stabilizer.tests.icache.helpers.RecordingCacheLoader;
import com.hazelcast.stabilizer.tests.icache.helpers.RecordingCacheWriter;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import java.io.Serializable;
import java.util.Random;

import static org.junit.Assert.assertNotNull;



/**
 * This tests concurrent load write and delete calls to CacheLoader. via put remove get calls to a cache
 * we can configure a delay in the load write and delete
 * a large delay and high concurrent calls to loadAll could overflow some internal queues
 * we Verify that the cache contains all keys,  and that the keys have been loaded through a loader instance
 * */
public class ReadWriteICacheTest {

    private final static ILogger log = Logger.getLogger(ReadWriteICacheTest.class);

    public int threadCount = 3;
    public int keyCount=10;
    public double putProb=0.4;
    public double getProb=0.4;
    public double removeProb=0.2;

    public int putDelayMs=0;
    public int getDelayMs=0;
    public int removeDealyMs=0;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private CacheManager cacheManager;
    private String basename;

    private MutableConfiguration config;
    private Cache<Object,Object> cache;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        basename = testContext.getTestId();

        if (TestUtils.isMemberNode(targetInstance)) {
            HazelcastServerCachingProvider hcp = new HazelcastServerCachingProvider();
            cacheManager = new HazelcastServerCacheManager(hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        } else {
            HazelcastClientCachingProvider hcp = new HazelcastClientCachingProvider();
            cacheManager = new HazelcastClientCacheManager(hcp, targetInstance, hcp.getDefaultURI(), hcp.getDefaultClassLoader(), null);
        }

        config = new MutableConfiguration();
        config.setReadThrough(true);
        config.setWriteThrough(true);

        RecordingCacheLoader loader = new RecordingCacheLoader();
        RecordingCacheWriter writer = new RecordingCacheWriter();

        loader.loadDelayMs = getDelayMs;
        writer.writeDelayMs = putDelayMs;
        writer.deleteDelayMs = removeDealyMs;

        config.setCacheLoaderFactory(FactoryBuilder.factoryOf( loader ));
        config.setCacheWriterFactory(FactoryBuilder.factoryOf( writer ));

        cacheManager.createCache(basename, config);
        cache = cacheManager.getCache(basename);
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
        private final Counter counter = new Counter();

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                int key = random.nextInt(keyCount);

                double chance = random.nextDouble();
                if ( (chance -= putProb) < 0 ) {
                    cache.put(key, key);
                    counter.put++;

                }
                else if ( (chance -= getProb) < 0 ) {
                    Object o = cache.get(key);
                    assertNotNull(o);
                    counter.get++;
                }
                else if ( (chance -= removeProb) < 0) {
                    cache.remove(key);
                    counter.remove++;
                }
            }
            targetInstance.getList(basename+"counters").add(counter);
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {

        RecordingCacheLoader loader = (RecordingCacheLoader) config.getCacheLoaderFactory().create();
        RecordingCacheWriter writer = (RecordingCacheWriter) config.getCacheWriterFactory().create();

        log.info(basename+": "+loader);
        log.info(basename+": "+writer);
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {

        IList<Counter> counters = targetInstance.getList(basename+"counters");
        Counter total = new Counter();
        for(Counter c : counters){
            total.add(c);
        }
        log.info(basename+": "+total+" from "+counters.size()+" worker threads");
    }

    static class Counter implements Serializable {

        public long put = 0;
        public long get = 0;
        public long remove = 0;

        public void add(Counter c) {
            put += c.put;
            get += c.get;
            remove += c.remove;
        }

        public String toString() {
            return "Counter{" +
                    "put=" + put +
                    ", get=" + get +
                    ", remove=" + remove +
                    '}';
        }
    }
}
