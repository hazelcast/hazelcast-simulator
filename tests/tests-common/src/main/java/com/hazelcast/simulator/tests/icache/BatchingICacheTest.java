/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.ICache;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import javax.cache.CacheManager;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;

/**
 * Demonstrates the effect of batching.
 *
 * It uses async methods to invoke operation and wait for future to complete every {@code batchSize} invocations.
 * Hence setting {@link #batchSize} to 1 is effectively the same as using sync operations.
 *
 * Setting {@link #batchSize} to values greater than 1 causes the batch-effect to kick-in, pipe-lines are utilized better
 * and overall throughput goes up.
 */
public class BatchingICacheTest extends AbstractTest {

    // properties
    public int keyCount = 1000000;
    public int batchSize = 1;

    private ICache<Object, Object> cache;

    @Setup
    public void setup() {
        CacheManager cacheManager = createCacheManager(targetInstance);
        cache = (ICache<Object, Object>) cacheManager.getCache(name);
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Object, Object> streamer = StreamerFactory.getInstance(cache);
        for (int i = 0; i < keyCount; i++) {
            streamer.pushEntry(i, 0);
        }
        streamer.await();
    }

    @TimeStep(prob = 0.1)
    public void write(ThreadState state) throws Exception {
        Integer key = state.randomInt(keyCount);
        Integer value = state.randomInt();
        ICompletableFuture<?> future = cache.putAsync(key, value);
        state.futureList.add(future);
        state.syncIfNecessary(state.iteration++);
    }

    @TimeStep(prob = -1)
    public void get(ThreadState state) throws Exception {
        Integer key = state.randomInt(keyCount);
        ICompletableFuture<?> future = cache.getAsync(key);
        state.futureList.add(future);
        state.syncIfNecessary(state.iteration++);
    }

    public class ThreadState extends BaseThreadState {

        private final List<ICompletableFuture<?>> futureList = new ArrayList<ICompletableFuture<?>>(batchSize);
        private long iteration;

        private void syncIfNecessary(long iteration) throws Exception {
            if (iteration % batchSize == 0) {
                for (ICompletableFuture<?> future : futureList) {
                    future.get();
                }
                futureList.clear();
            }
        }
    }

    @Teardown
    public void teardown() {
        cache.close();
    }
}
