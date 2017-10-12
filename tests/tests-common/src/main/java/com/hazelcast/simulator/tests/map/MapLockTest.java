/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link IMap#lock(Object)} method.
 *
 * We use {@link IMap#lock(Object)} to control concurrent access to map key/value pairs. There are a total of {@link #keyCount}
 * keys stored in a map which are initialized to zero, we concurrently increment the value of a random key. We keep track of all
 * increments to each key and verify the value in the map for each key is equal to the total increments done on each key.
 */
public class MapLockTest extends AbstractTest {

    // properties
    public int keyCount = 1000;

    private IMap<Integer, Long> map;
    private IList<long[]> incrementsList;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        incrementsList = targetInstance.getList(name);
    }

    @Prepare(global = true)
    public void prepare() {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0L);
        }
    }

    @TimeStep
    public void timeStep(ThreadState state) {
        int key = state.randomInt(keyCount);
        long increment = state.randomInt(100);

        map.lock(key);
        try {
            Long current = map.get(key);
            map.put(key, current + increment);
            state.increments[key] += increment;
        } finally {
            map.unlock(key);
        }
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        incrementsList.add(state.increments);
    }

    public class ThreadState extends BaseThreadState {

        private final long[] increments = new long[keyCount];
    }

    @Verify
    public void globalVerify() {
        long[] expected = new long[keyCount];

        for (long[] increments : incrementsList) {
            for (int i = 0; i < keyCount; i++) {
                expected[i] += increments[i];
            }
        }
        logger.info(format("%s: collected increments from %d worker threads", name, incrementsList.size()));

        int failures = 0;
        for (int i = 0; i < keyCount; i++) {
            if (expected[i] != map.get(i)) {
                failures++;
            }
        }
        assertEquals(format("%s: %d keys have been incremented unexpectedly out of %d keys", name, failures, keyCount), 0,
                failures);
    }
}
