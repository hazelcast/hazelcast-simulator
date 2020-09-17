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
package com.hazelcast.simulator.infinispan11.cache;

import com.hazelcast.simulator.infinispan11.InfinispanTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.InjectDriver;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.infinispan.commons.api.BasicCacheContainer;

import java.util.Map;

public class LongLongCacheTest extends InfinispanTest {

    // properties
    public int keyDomain = 10000;

    @InjectDriver
    private BasicCacheContainer cacheContainer;
    private Map<Long, Long> cache;

    @Setup
    public void setup() {
        cache = cacheContainer.getCache(name);
    }

    @Prepare(global = true)
    public void prepare() {
        for (long key = 0; key < keyDomain; key++) {
            cache.put(key, key);
        }
    }

    @TimeStep(prob = -1)
    public Long get(ThreadState state) {
        return cache.get(state.randomKey());
    }

    @TimeStep(prob = 0.1)
    public void put(ThreadState state) {
        cache.put(state.randomKey(), state.randomValue());
    }

    public class ThreadState extends BaseThreadState {

        private Long randomKey() {
            return randomLong(keyDomain);
        }

        private Long randomValue() {
            return randomLong();
        }
    }
}
