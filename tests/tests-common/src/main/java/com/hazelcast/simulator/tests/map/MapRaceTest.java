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
package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.BaseThreadContext;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.AbstractTest;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.utils.UuidUtil.newSecureUuidString;
import static org.junit.Assert.assertEquals;

/**
 * This test verifies that there are race problems if the {@link IMap} is not used correctly.
 * <p>
 * This test is expected to fail.
 */
public class MapRaceTest extends AbstractTest {

    // properties
    public int keyCount = 1000;

    private IMap<Integer, Long> map;
    private IMap<String, Map<Integer, Long>> resultMap;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        resultMap = targetInstance.getMap(name + ":ResultMap");
    }

    @Warmup(global = true)
    public void warmup() {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0L);
        }
    }



    @TimeStep
    public void timeStep(ThreadContext context) {
        Integer key = context.randomInt(keyCount);
        long increment = context.randomInt(100);

        context.incrementMap(map, key, increment);
        context.incrementMap(context.result, key, increment);
    }

    @AfterRun
    public void afterRun(ThreadContext context) {
        resultMap.put(newSecureUuidString(), context.result);
    }

    public class ThreadContext extends BaseThreadContext {
        private final Map<Integer, Long> result = new HashMap<Integer, Long>();

        ThreadContext() {
            for (int i = 0; i < keyCount; i++) {
                result.put(i, 0L);
            }
        }

        private void incrementMap(Map<Integer, Long> map, Integer key, long increment) {
            map.put(key, map.get(key) + increment);
        }
    }

    @Verify
    public void verify() {
        long[] expected = new long[keyCount];
        for (Map<Integer, Long> result : resultMap.values()) {
            for (Map.Entry<Integer, Long> increments : result.entrySet()) {
                expected[increments.getKey()] += increments.getValue();
            }
        }

        int failures = 0;
        for (int i = 0; i < keyCount; i++) {
            long actual = map.get(i);
            if (expected[i] != actual) {
                failures++;
            }
        }

        assertEquals("There should not be any data races", 0, failures);
    }

    @Teardown
    public void tearDown() {
        map.destroy();
        resultMap.destroy();
    }
}
