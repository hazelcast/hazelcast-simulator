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
package com.hazelcast.simulator.ignite.cache;

import com.hazelcast.simulator.ignite.IgniteTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;

import javax.cache.Cache;

public class CacheTest extends IgniteTest {

    // properties
    public int keyCount = 10000;

    private Cache<Object, Object> cache;

    @Setup
    public void setup() {
        cache = ignite.getOrCreateCache(name);
    }

    @Prepare(global = true)
    public void prepare() {
        for (int k = 0; k < keyCount; k++) {
            cache.put(k, 0);
        }
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
