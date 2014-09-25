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
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.icache.helpers.RecordingCacheLoader;
import com.hazelcast.stabilizer.tests.icache.helpers.RecordingCacheWriter;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import junit.framework.TestCase;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CompletionListenerFuture;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertFalse;


public class ReadWriteICacheTest {

    private final static ILogger log = Logger.getLogger(ReadWriteICacheTest.class);

    public int threadCount = 3;
    public int initalKeyLoad=10;
    public int keyCount=10;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private HazelcastCacheManager cacheManager;
    private String basename;

    private CacheConfig config;
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

        config = new CacheConfig();
        config.setName(basename);
        config.setTypes(Object.class, Object.class);
        config.setReadThrough(true);
        config.setWriteThrough(true);
        RecordingCacheLoader a = new RecordingCacheLoader();
        a.load=true;
        config.setCacheLoaderFactory(FactoryBuilder.factoryOf( a ));
        config.setCacheWriterFactory(FactoryBuilder.factoryOf( new RecordingCacheWriter() ));

        cacheManager.createCache(basename, config);
        cache = cacheManager.getCache(basename);
        config = cache.getConfiguration(CacheConfig.class);
    }

    @Warmup(global = true)
    public void warmup() throws Exception {

        Set keySet = new HashSet();
        for(int i=0; i<initalKeyLoad; i++){
            keySet.add(i);
        }

        CompletionListenerFuture loaded = new CompletionListenerFuture();
        cache.loadAll(keySet, true, loaded);
        loaded.get();
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
                int key = random.nextInt(keyCount) + initalKeyLoad;
                cache.put(key, key);
                counter.put++;
            }
            RecordingCacheLoader loader = (RecordingCacheLoader) config.getCacheLoaderFactory().create();
            targetInstance.getList(basename+"loaders").add(loader);
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


        for(int k=0; k<initalKeyLoad; k++){
            assertTrue(basename + ": key "+k+" not in cache", cache.containsKey(k));
        }

        boolean[] loaded = new boolean[initalKeyLoad];
        Arrays.fill(loaded, false);

        IList<RecordingCacheLoader> loaders = targetInstance.getList(basename+"loaders");
        for(RecordingCacheLoader loader : loaders){
            for(int k=0; k<initalKeyLoad; k++){
                if(loader.hasLoaded(k)){
                    assertFalse(basename+ ": key "+k+" loaded twice", loaded[k]);
                    loaded[k]=true;
                }
            }
        }

        for(int k=0; k<initalKeyLoad; k++){
            assertTrue(basename+": Key "+k+" not in loader", loaded[k]);
        }
    }

    static class Counter implements Serializable {

        public long put = 0;

        public void add(Counter c) {
            put += c.put;
        }

        @Override
        public String toString() {
            return "Counter{" +
                    "put=" + put +
                    '}';
        }
    }

}
