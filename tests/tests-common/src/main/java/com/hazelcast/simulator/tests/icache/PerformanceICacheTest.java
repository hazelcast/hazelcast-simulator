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

import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;

/**
 * A performance test for the cache. The key is integer and value is a integer
 */
public class PerformanceICacheTest extends AbstractTest {

    // properties
    public int keyCount = 1000000;

    private Cache<Object, Object> cache;

    @Setup
    public void setup() {
        CacheManager cacheManager = createCacheManager(targetInstance);
        cache = cacheManager.getCache(name);
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
    public void put(ThreadState state) {
        Integer key = state.randomInt(keyCount);
        cache.put(key, state.value++);
    }

    @TimeStep(prob = -1)
    public void get(ThreadState state) {
        Integer key = state.randomInt(keyCount);
        cache.get(key);
    }

    public class ThreadState extends BaseThreadState {
        private int value;
    }

    @Teardown
    public void teardown() {
        cache.close();
    }
}
