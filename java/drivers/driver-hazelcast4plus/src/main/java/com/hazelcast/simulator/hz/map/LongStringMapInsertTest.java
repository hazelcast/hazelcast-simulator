package com.hazelcast.simulator.hz.map;

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

import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.StopException;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateAsciiStrings;


public class LongStringMapInsertTest extends HazelcastTest {

    // properties
    public int keyCountPerLoadGenerator = 10000;
    public int uniqueValues = 10000;
    public int minValueLength = 10;
    public int maxValueLength = 10;
    public int threadCount;
    public boolean destroyOnExit = true;

    private IMap<Long, String> map;
    private String[] values;
    private IAtomicLong keyGenerator;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        values = generateAsciiStrings(uniqueValues, minValueLength, maxValueLength);
        keyGenerator = getAtomicLong("key-generator");
    }

    @TimeStep(prob = 1)
    public void set(ThreadState state) {
        if (!state.initialized) {
            state.remaining = keyCountPerLoadGenerator / threadCount;
            state.nextKey = keyGenerator.getAndAdd(state.remaining);
            state.initialized = true;
        }

        if (state.remaining > 0) {
            map.set(state.nextKey, state.randomValue());
            state.remaining--;
            state.nextKey++;
        } else {
            throw new StopException();
        }
    }

    public class ThreadState extends BaseThreadState {

        private boolean initialized;
        private long nextKey;
        private long remaining;

        private String randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown
    public void tearDown() {
        long start = System.currentTimeMillis();
        int size = map.size();
        long duration = System.currentTimeMillis() - start;
        System.out.println("Map.size:" + size + " duration:" + duration + " ms");

        if (destroyOnExit) {
            map.destroy();
        }
    }
}

