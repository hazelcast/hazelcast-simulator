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
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.integration.CompletionListenerFuture;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertFalse;

/**
 * This tests concurrent load all calls to CacheLoader.
 * we can configure a delay in the loadAll method of the CacheLoader
 * we can configure to wait for loadAll completion boolean
 * a large delay and high concurrent calls to loadAll could overflow some internal queues
 * if waitForLoadAllFutureComplition is false, again we could overflow some internal queues
 * we Verify that the cache contains all keys,  and that the keys have been loaded through a loader instance
 * */
public class CacheLoaderTest {

    private final static ILogger log = Logger.getLogger(CacheLoaderTest.class);

    public int threadCount = 3;
    public int keyCount = 10;
    public int loadAllDelayMs = 0;
    public boolean waitForLoadAllFutureComplition = true;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private HazelcastCacheManager cacheManager;
    private String basename;

    private MutableConfiguration config;
    private Cache<Object,Object> cache;
    private Set keySet = new HashSet();

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

        RecordingCacheLoader recordingCacheLoader = new RecordingCacheLoader();
        recordingCacheLoader.loadAllDelayMs = loadAllDelayMs;

        config.setCacheLoaderFactory(FactoryBuilder.factoryOf( recordingCacheLoader ));

        cacheManager.createCache(basename, config);
        cache = cacheManager.getCache(basename);
    }

    @Warmup(global = false)
    public void warmup(){
        for(int i=0; i< keyCount; i++){
            keySet.add(i);
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
        public void run() {
            while (!testContext.isStopped()) {

                CompletionListenerFuture loaded = new CompletionListenerFuture();
                cache.loadAll(keySet, true, loaded);

                if ( waitForLoadAllFutureComplition ) {
                    try {
                        loaded.get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            RecordingCacheLoader loader = (RecordingCacheLoader) config.getCacheLoaderFactory().create();
            targetInstance.getList(basename+"loaders").add(loader);
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        RecordingCacheLoader loader = (RecordingCacheLoader) config.getCacheLoaderFactory().create();
        log.info(basename+": "+loader);
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {

        for(int k=0; k< keyCount; k++){
            assertTrue(basename + ": cache should contain key "+k, cache.containsKey(k) );
        }

        IList<RecordingCacheLoader> loaders = targetInstance.getList(basename+"loaders");

        boolean[] loaded = new boolean[keyCount];
        Arrays.fill(loaded, false);
        for(RecordingCacheLoader loader : loaders){
            for(int k=0; k< keyCount; k++){
                if(loader.hasLoaded(k)){
                    loaded[k]=true;
                }
            }
        }

        for(int i=0; i< keyCount; i++){
            assertTrue(basename+": Key "+i+" not in loader",loaded[i]);
        }
    }
}
