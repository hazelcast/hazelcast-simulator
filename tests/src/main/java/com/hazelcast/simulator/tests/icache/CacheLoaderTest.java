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
package com.hazelcast.simulator.tests.icache;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.icache.helpers.RecordingCacheLoader;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.integration.CompletionListenerFuture;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;
import static org.junit.Assert.assertTrue;

/**
 * This tests concurrent load all calls to {@link javax.cache.integration.CacheLoader}.
 *
 * We can configure a delay in {@link javax.cache.integration.CacheLoader#loadAll(Iterable)} or to wait for completion.
 *
 * A large delay and high concurrent calls to {@link javax.cache.integration.CacheLoader#loadAll(Iterable)} could overflow some
 * internal queues. The same can happen if {@link #waitForLoadAllFutureCompletion} is false.
 *
 * We verify that the cache contains all keys and that the keys have been loaded through a the cache instance.
 */
public class CacheLoaderTest {

    private static final ILogger LOGGER = Logger.getLogger(CacheLoaderTest.class);

    public String basename = CacheLoaderTest.class.getSimpleName();
    public int keyCount = 10;
    public int loadAllDelayMs = 0;
    public boolean waitForLoadAllFutureCompletion = true;

    private final Set<Integer> keySet = new HashSet<Integer>();

    private IList<RecordingCacheLoader<Integer>> loaderList;
    private MutableConfiguration<Integer, Integer> config;
    private Cache<Integer, Integer> cache;

    @Setup
    public void setup(TestContext testContext) {
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
        loaderList = hazelcastInstance.getList(basename + "loaders");

        config = new MutableConfiguration<Integer, Integer>();
        config.setReadThrough(true);

        RecordingCacheLoader<Integer> recordingCacheLoader = new RecordingCacheLoader<Integer>();
        recordingCacheLoader.loadAllDelayMs = loadAllDelayMs;
        config.setCacheLoaderFactory(FactoryBuilder.factoryOf(recordingCacheLoader));

        CacheManager cacheManager = createCacheManager(hazelcastInstance);
        cacheManager.createCache(basename, config);
        cache = cacheManager.getCache(basename);
    }

    @Warmup(global = false)
    public void warmup() {
        for (int i = 0; i < keyCount; i++) {
            keySet.add(i);
        }
    }

    @Verify(global = false)
    public void verify() {
        RecordingCacheLoader<Integer> loader = (RecordingCacheLoader<Integer>) config.getCacheLoaderFactory().create();
        LOGGER.info(basename + ": " + loader);
    }

    @Verify(global = true)
    public void globalVerify() {
        for (int i = 0; i < keyCount; i++) {
            assertTrue(basename + ": cache should contain key " + i, cache.containsKey(i));
        }

        boolean[] loaded = new boolean[keyCount];
        Arrays.fill(loaded, false);
        for (RecordingCacheLoader<Integer> loader : loaderList) {
            for (int i = 0; i < keyCount; i++) {
                if (loader.hasLoaded(i)) {
                    loaded[i] = true;
                }
            }
        }

        for (int i = 0; i < keyCount; i++) {
            assertTrue(basename + ": key " + i + " not in loader", loaded[i]);
        }
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        @Override
        public void timeStep() throws Exception {
            CompletionListenerFuture loaded = new CompletionListenerFuture();
            cache.loadAll(keySet, true, loaded);

            if (waitForLoadAllFutureCompletion) {
                loaded.get();
            }
        }

        @Override
        protected void afterRun() {
            RecordingCacheLoader<Integer> loader = (RecordingCacheLoader<Integer>) config.getCacheLoaderFactory().create();
            loaderList.add(loader);
        }
    }
}
