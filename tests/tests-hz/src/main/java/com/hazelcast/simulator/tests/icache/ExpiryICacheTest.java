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

import com.hazelcast.cache.ICache;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.utils.AssertTask;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.getCache;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;

public class ExpiryICacheTest extends HazelcastTest {

    // default keyCount entries of int, is upper bound to approx 8MB possible max, if all put inside expiryPolicy time
    public int keyCount = 1000000;

    private final ExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(Duration.ONE_MINUTE);

    private ICache<Integer, Integer> cache;

    @Setup
    public void setup() {
        cache = getCache(targetInstance, name);
    }

    @TimeStep
    public void timeStep(BaseThreadState state) {
        int key = state.randomInt(keyCount);
        if (!cache.containsKey(key)) {
            cache.put(key, 0, expiryPolicy);
        }
    }

    @Verify(global = true)
    public void globalVerify() {
        sleepSeconds(61);

        // provoke expire after TTL
        for (int i = 0; i < keyCount; i++) {
            cache.containsKey(i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int cacheSize = cache.size();
                logger.info(name + " ICache size: " + cacheSize);
                assertEquals(name + " ICache should be empty, but TTL events are not processed", 0, cacheSize);
            }
        });
    }
}
