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

import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateStrings;

public class StringICacheTest extends AbstractTest {

    // properties
    public int keyLength = 10;
    public int keyCount = 10000;
    public int valueCount = 10000;
    public int valueLength = -1;
    public int minValueLength = 10;
    public int maxValueLength = 10;

    // if we use the putAndGet (so returning a value) or the put (which returns void)
    public boolean useGetAndPut = true;
    public KeyLocality keyLocality = KeyLocality.SHARED;
    public int minNumberOfMembers = 0;

    private Cache<String, String> cache;
    private String[] keys;
    private String[] values;

    @Setup
    public void setup() {
        CacheManager cacheManager = createCacheManager(targetInstance);
        cache = cacheManager.getCache(name);
    }

    @Prepare
    public void prepare() {
        waitClusterSize(logger, targetInstance, minNumberOfMembers);

        keys = generateStringKeys(keyCount, keyLength, keyLocality, targetInstance);

        if (valueLength > -1) {
            minValueLength = valueLength;
            maxValueLength = valueLength;
        }

        values = generateStrings(valueCount, minValueLength, maxValueLength);

        Random random = new Random();
        Streamer<String, String> streamer = StreamerFactory.getInstance(cache);
        for (String key : keys) {
            String value = values[random.nextInt(valueCount)];
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @TimeStep(prob = 0.1)
    public void put(ThreadState state) {
        String key = state.randomKey();
        String value = state.randomValue();
        if (useGetAndPut) {
            cache.getAndPut(key, value);
        } else {
            cache.put(key, value);
        }
    }

    @TimeStep(prob = -1)
    public void get(ThreadState state) {
        String key = state.randomKey();
        cache.get(key);
    }

    public class ThreadState extends BaseThreadState {

        private String randomValue() {
            return values[randomInt(values.length)];
        }

        private String randomKey() {
            int length = keys.length;
            return keys[randomInt(length)];
        }
    }

    @Teardown
    public void teardown() {
        cache.close();
    }
}
