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
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
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
 * This tests the cas method: replace. So for optimistic concurrency control.
 * <p>
 * We have a bunch of predefined keys, and we are going to concurrently increment the value and we protect ourselves against lost
 * updates using cas method replace.
 * <p>
 * Locally we keep track of all increments, and if the sum of these local increments matches the global increment, we are done.
 */
public class MapCasTest extends AbstractTest {

    // properties
    public int keyCount = 1000;

    private IMap<Integer, Long> map;
    private IMap<String, Map<Integer, Long>> resultsPerWorker;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        resultsPerWorker = targetInstance.getMap(name + ":ResultMap");
    }

    @Warmup(global = true)
    public void warmup() {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0L);
        }
    }

    @BeforeRun
    public void beforeRun(ThreadContext context) {
        int size = map.size();
        if (size != keyCount) {
            throw new TestException("Warmup has not run since the map is not filled correctly, "
                    + "found size: %s, expected size: %s", size, keyCount);
        }
        for (int i = 0; i < keyCount; i++) {
            context.result.put(i, 0L);
        }
    }

    @TimeStep
    public void timeStep(ThreadContext context) {
        Integer key = context.randomInt(keyCount);
        long incrementValue = context.randomInt(100);

        for (; ; ) {
            Long current = map.get(key);
            Long update = current + incrementValue;
            if (map.replace(key, current, update)) {
                context.result.put(key, context.result.get(key) + incrementValue);
                break;
            }
        }
    }

    @AfterRun
    public void afterRun(ThreadContext context) {
        resultsPerWorker.put(newSecureUuidString(), context.result);
    }

    public class ThreadContext extends BaseThreadContext {
        Map<Integer, Long> result = new HashMap<Integer, Long>();
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

    @Teardown
    public void tearDown() {
        map.destroy();
        resultsPerWorker.destroy();
    }


}
