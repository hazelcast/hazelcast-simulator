/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.ucd.map;

import com.hazelcast.collection.IList;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.helpers.KeyUtils;
import com.hazelcast.simulator.tests.ucd.UCDTest;

import static org.junit.Assert.assertEquals;

public class TestUserCodeWithIMapEntryProcessor extends UCDTest {
    private IMap<Integer, Long> map;
    public KeyLocality keyLocality = KeyLocality.SHARED;
    public int keyCount = 10000;
    private IList<long[]> resultsPerWorker;

    private int[] keys;

    @Setup
    public void setUp() throws ReflectiveOperationException {
        super.setUp();
        map = targetInstance.getMap(name);
        keys = KeyUtils.generateIntKeys(keyCount, keyLocality, targetInstance);
        resultsPerWorker = targetInstance.getList(name + ":ResultMap");
    }

    @Prepare(global = true)
    public void prepare() {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0L);
        }
    }

    @TimeStep
    public void timeStep(ThreadState state) throws Exception {
        int key = keys[state.randomInt(keys.length)];

        long increment = state.randomInt(100);
        map.executeOnKey(key, (EntryProcessor<Integer, Long, Object>)
                udf.getDeclaredConstructor(long.class).newInstance(key));

        state.localIncrementsAtKey[key] += increment;
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        // sleep to give time for the last EntryProcessor tasks to complete
        resultsPerWorker.add(state.localIncrementsAtKey);
    }

    public class ThreadState extends BaseThreadState {
        private final long[] localIncrementsAtKey = new long[keyCount];
    }

    @Verify
    public void verify() {
        long[] expectedValueForKey = new long[keyCount];

        for (long[] incrementsAtKey : resultsPerWorker) {
            for (int i = 0; i < incrementsAtKey.length; i++) {
                expectedValueForKey[i] += incrementsAtKey[i];
            }
        }

        int failures = 0;
        for (int i = 0; i < keyCount; i++) {
            long expected = expectedValueForKey[i];
            long found = map.get(i);
            if (expected != found) {
                failures++;
            }
        }

        assertEquals(0, failures);
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
