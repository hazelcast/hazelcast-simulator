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
package com.hazelcast.simulator.infinispan.cache;

import com.hazelcast.simulator.infinispan.InfinispanTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import java.util.Random;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateStrings;

public class LongStringCacheTest extends InfinispanTest {

    // properties
    public int keyDomain = 10000;
    public int valueCount = 10000;
    public int valueLength = 10;
    public int minValueLength = valueLength;
    public int maxValueLength = valueLength;

    private Cache<Long, String> cache;
    private String[] values;

    @Setup
    public void setup() {
        CacheManager cacheManager = Caching.getCachingProvider().getCacheManager();
        cache = cacheManager.getCache(name);
        values = generateStrings(valueCount, minValueLength, maxValueLength);
    }

    @Prepare(global = true)
    public void prepare() {
        Random random = new Random();
        for (long key = 0; key < keyDomain; key++) {
            String value = values[random.nextInt(valueCount)];
            cache.put(key, value);
        }
    }

    @TimeStep(prob = -1)
    public String get(ThreadState state) {
        return cache.get(state.randomKey());
    }

    @TimeStep(prob = 0.1)
    public void put(ThreadState state) {
        cache.put(state.randomKey(), state.randomValue());
    }

    public class ThreadState extends BaseThreadState {

        private long randomKey() {
            return randomLong(keyDomain);
        }

        private String randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown
    public void tearDown() {
        cache.close();
    }
}
