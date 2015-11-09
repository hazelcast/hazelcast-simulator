/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * This test verifies that there are race problems if the {@link IMap} is not used correctly.
 *
 * This test is expected to fail.
 */
public class MapRaceTest {

    // properties
    public int threadCount = 10;
    public String basename = MapRaceTest.class.getSimpleName();
    public int keyCount = 1000;

    private IMap<Integer, Long> map;
    private IMap<String, Map<Integer, Long>> resultMap;

    @Setup
    public void setUp(TestContext testContext) {
        HazelcastInstance targetInstance = testContext.getTargetInstance();

        map = targetInstance.getMap(basename);
        resultMap = targetInstance.getMap(basename + ":ResultMap");
    }

    @Teardown
    public void tearDown() {
        map.destroy();
        resultMap.destroy();
    }

    @Warmup(global = true)
    public void warmup() {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0L);
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

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {
        private final Map<Integer, Long> result = new HashMap<Integer, Long>();

        @Override
        protected void beforeRun() {
            for (int i = 0; i < keyCount; i++) {
                result.put(i, 0L);
            }
        }

        @Override
        public void timeStep() {
            Integer key = randomInt(keyCount);
            long increment = randomInt(100);

            incrementMap(map, key, increment);
            incrementMap(result, key, increment);
        }

        @Override
        protected void afterRun() {
            resultMap.put(UUID.randomUUID().toString(), result);
        }

        private void incrementMap(Map<Integer, Long> map, Integer key, long increment) {
            map.put(key, map.get(key) + increment);
        }
    }

    public static void main(String[] args) throws Exception {
        MapRaceTest test = new MapRaceTest();
        new TestRunner<MapRaceTest>(test).run();
    }
}
