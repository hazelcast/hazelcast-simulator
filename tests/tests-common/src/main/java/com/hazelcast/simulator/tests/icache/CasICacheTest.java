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

import com.hazelcast.core.IList;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;
import static org.junit.Assert.assertEquals;

/**
 * Tests the cas method {@link Cache#replace(Object, Object, Object)} for optimistic concurrency control.
 *
 * With a collection of predefined keys we concurrently increment the value.
 * We protect ourselves against lost updates using the cas method {@link Cache#replace(Object, Object, Object)}.
 *
 * Locally we keep track of all increments. We verify if the sum of these local increments matches the global increment.
 */
public class CasICacheTest extends AbstractTest {

    public int keyCount = 1000;

    private IList<long[]> resultsPerWorker;
    private Cache<Integer, Long> cache;

    @Setup
    public void setup() {
        resultsPerWorker = targetInstance.getList(name);

        CacheManager cacheManager = createCacheManager(targetInstance);
        cache = cacheManager.getCache(name);
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Integer, Long> streamer = StreamerFactory.getInstance(cache);
        for (int i = 0; i < keyCount; i++) {
            streamer.pushEntry(i, 0L);
        }
        streamer.await();
    }

    @TimeStep
    public void timeStep(ThreadState state) {
        int key = state.randomInt(keyCount);
        long increment = state.randomInt(100);

        while (true) {
            Long current = cache.get(key);
            if (cache.replace(key, current, current + increment)) {
                state.increments[key] += increment;
                break;
            }
        }
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        resultsPerWorker.add(state.increments);
    }

    public class ThreadState extends BaseThreadState {

        private final long[] increments = new long[keyCount];
    }

    @Verify
    public void verify() {
        long[] amount = new long[keyCount];
        for (long[] increments : resultsPerWorker) {
            for (int i = 0; i < keyCount; i++) {
                amount[i] += increments[i];
            }
        }

        int failures = 0;
        for (int i = 0; i < keyCount; i++) {
            long expected = amount[i];
            long found = cache.get(i);
            if (expected != found) {
                failures++;
            }
        }
        assertEquals(failures + " key=>values have been incremented unExpected", 0, failures);
    }

    @Teardown
    public void teardown() {
        cache.close();
        resultsPerWorker.destroy();
    }
}
