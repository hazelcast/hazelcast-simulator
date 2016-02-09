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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.utils.UuidUtil.newSecureUuidString;
import static org.junit.Assert.assertEquals;

/**
 * This tests the cas method: replace. So for optimistic concurrency control.
 *
 * We have a bunch of predefined keys, and we are going to concurrently increment the value and we protect ourselves against lost
 * updates using cas method replace.
 *
 * Locally we keep track of all increments, and if the sum of these local increments matches the global increment, we are done.
 */
public class MapCasTest {

    // properties
    public int keyCount = 1000;
    public String basename = MapCasTest.class.getSimpleName();

    private IMap<Integer, Long> map;
    private IMap<String, Map<Integer, Long>> resultsPerWorker;

    @Setup
    public void setUp(TestContext testContext) {
        HazelcastInstance targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
        resultsPerWorker = targetInstance.getMap(basename + ":ResultMap");
    }

    @Teardown
    public void tearDown() {
        map.destroy();
        resultsPerWorker.destroy();
    }

    @Warmup(global = true)
    public void warmup() {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0L);
        }
    }

    @Verify
    public void verify() {
        long[] amount = new long[keyCount];

        for (Map<Integer, Long> workerResult : resultsPerWorker.values()) {
            for (Map.Entry<Integer, Long> entry : workerResult.entrySet()) {
                amount[entry.getKey()] += entry.getValue();
            }
        }

        int failures = 0;
        for (int i = 0; i < keyCount; i++) {
            long expected = amount[i];
            long found = map.get(i);
            if (expected != found) {
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

        protected void beforeRun() {
            int size = map.size();
            if (size != keyCount) {
                throw new TestException("Warmup has not run since the map is not filled correctly, found size: %s", size);
            }
            for (int i = 0; i < keyCount; i++) {
                result.put(i, 0L);
            }
        }

        @Override
        protected void timeStep() throws Exception {
            Integer key = randomInt(keyCount);
            long incrementValue = randomInt(100);

            for (;;) {
                Long current = map.get(key);
                Long update = current + incrementValue;
                if (map.replace(key, current, update)) {
                    increment(key, incrementValue);
                    break;
                }
            }
        }

        protected void afterRun() {
            resultsPerWorker.put(newSecureUuidString(), result);
        }

        private void increment(int key, long increment) {
            result.put(key, result.get(key) + increment);
        }
    }

    public static void main(String[] args) throws Exception {
        MapCasTest test = new MapCasTest();
        new TestRunner<MapCasTest>(test).run();
    }
}
