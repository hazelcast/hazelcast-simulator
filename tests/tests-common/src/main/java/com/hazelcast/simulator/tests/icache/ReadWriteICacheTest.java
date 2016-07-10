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
package com.hazelcast.simulator.tests.icache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.IList;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.tests.icache.helpers.ICacheReadWriteCounter;
import com.hazelcast.simulator.tests.icache.helpers.RecordingCacheLoader;
import com.hazelcast.simulator.tests.icache.helpers.RecordingCacheWriter;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.FactoryBuilder;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;
import static org.junit.Assert.assertNotNull;

/**
 * This tests concurrent load write and delete calls to CacheLoader. Via put, remove and get calls to a cache
 * we can configure a delay in the load write and delete.
 * A large delay and high concurrent calls to loadAll could overflow some internal queues.
 * We verify that the cache contains all keys and that the keys have been loaded through a loader instance.
 */
public class ReadWriteICacheTest extends AbstractTest {

    private enum Operation {
        PUT,
        GET,
        REMOVE
    }

    public int keyCount = 10;
    public double putProb = 0.4;
    public double getProb = 0.4;
    public double removeProb = 0.2;

    public int putDelayMs = 0;
    public int getDelayMs = 0;
    public int removeDelayMs = 0;

    private final OperationSelectorBuilder<Operation> builder = new OperationSelectorBuilder<Operation>();

    private IList<ICacheReadWriteCounter> counters;
    private CacheConfig<Integer, Integer> config;
    private Cache<Integer, Integer> cache;

    @Setup
    public void setup() {
        counters = targetInstance.getList(name + "counters");

        RecordingCacheLoader<Integer> loader = new RecordingCacheLoader<Integer>();
        loader.loadDelayMs = getDelayMs;

        RecordingCacheWriter<Integer, Integer> writer = new RecordingCacheWriter<Integer, Integer>();
        writer.writeDelayMs = putDelayMs;
        writer.deleteDelayMs = removeDelayMs;

        config = new CacheConfig<Integer, Integer>();
        config.setReadThrough(true);
        config.setWriteThrough(true);
        config.setCacheLoaderFactory(FactoryBuilder.factoryOf(loader));
        config.setCacheWriterFactory(FactoryBuilder.factoryOf(writer));

        CacheManager cacheManager = createCacheManager(targetInstance);
        cacheManager.createCache(name, config);
        cache = cacheManager.getCache(name);

        builder.addOperation(Operation.PUT, putProb)
                .addOperation(Operation.GET, getProb)
                .addOperation(Operation.REMOVE, removeProb);
    }

    @RunWithWorker
    public Worker run() {
        return new Worker();
    }

    private final class Worker extends AbstractWorker<Operation> {

        private final ICacheReadWriteCounter counter = new ICacheReadWriteCounter();

        private Worker() {
            super(builder);
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            int key = randomInt(keyCount);

            switch (operation) {
                case PUT:
                    cache.put(key, key);
                    counter.put++;
                    break;

                case GET:
                    Object o = cache.get(key);
                    assertNotNull(o);
                    counter.get++;
                    break;

                case REMOVE:
                    cache.remove(key);
                    counter.remove++;
                    break;

                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        public void afterRun() {
            counters.add(counter);
        }
    }

    @Verify(global = false)
    public void verify() {
        RecordingCacheLoader loader = (RecordingCacheLoader) config.getCacheLoaderFactory().create();
        RecordingCacheWriter writer = (RecordingCacheWriter) config.getCacheWriterFactory().create();

        logger.info(name + ": " + loader);
        logger.info(name + ": " + writer);
    }

    @Verify(global = true)
    public void globalVerify() {
        ICacheReadWriteCounter total = new ICacheReadWriteCounter();
        for (ICacheReadWriteCounter counter : counters) {
            total.add(counter);
        }
        logger.info(name + ": " + total + " from " + counters.size() + " worker threads");
    }
}
