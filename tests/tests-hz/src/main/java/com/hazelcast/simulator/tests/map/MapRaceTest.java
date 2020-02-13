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

import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.utils.UuidUtil.newSecureUuidString;
import static org.junit.Assert.assertEquals;

/**
 * This test verifies that there are race problems if the {@link IMap} is not used correctly.
 *
 * This test is expected to fail.
 */
public class MapRaceTest extends HazelcastTest {

    // properties
    public int keyCount = 1000;

    private IMap<Integer, Long> map;
    private IMap<String, Map<Integer, Long>> resultMap;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        resultMap = targetInstance.getMap(name + ":ResultMap");
    }

    @Prepare(global = true)
    public void prepare() {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0L);
        }
    }

    @BeforeRun
    public void beforeRun(ThreadState state) {
        for (int i = 0; i < keyCount; i++) {
            state.result.put(i, 0L);
        }
    }

    @TimeStep
    public void timeStep(ThreadState state) {
        Integer key = state.randomInt(keyCount);
        long increment = state.randomInt(100);

        state.incrementMap(map, key, increment);
        state.incrementMap(state.result, key, increment);
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        resultMap.put(newSecureUuidString(), state.result);
    }

    public class ThreadState extends BaseThreadState {

        private final Map<Integer, Long> result = new HashMap<>();

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
