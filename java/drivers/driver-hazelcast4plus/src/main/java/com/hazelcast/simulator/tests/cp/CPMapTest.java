/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.cp;

import com.hazelcast.cp.CPMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.GeneratorUtils;

import java.util.UUID;

/**
 * Note that these tests are designed to be run in isolation w.r.t. one another.
 */
public class CPMapTest extends HazelcastTest {
    // number of distinct keys to create and use during the tests
    public int distinctKeys = 1;
    // size in bytes for each key's value
    public int valueSizeBytes = 1024;
    private String[] keys;
    private CPMap<String, String> map;

    private String v; // this is always the value associated with any key; exception is remove and delete


    @Setup
    public void setup() {
        v = createString(valueSizeBytes);
        map = targetInstance.getCPSubsystem().getMap("map");
        for (int i = 0; i < distinctKeys; i++) {
            keys[i] = UUID.randomUUID().toString();
        }
    }

    @Prepare(global = true)
    public void prepare() {
        for (String key : keys) {
            map.set(key, v);
        }
    }

    String createString(int bytes) {
        // as it's ascii
        return GeneratorUtils.generateAsciiString(bytes);
    }

    @TimeStep(prob = 1)
    public void set(ThreadState state) {
        map.set(state.randomKey(), v);
    }

    @TimeStep(prob = 0)
    public String put(ThreadState state) {
        return map.put(state.randomKey(), v);
    }

    @TimeStep(prob = 0)
    public String get(ThreadState state) {
        return map.get(state.randomKey());
    }

    // 'remove' and 'delete' other than their first invocation pointless -- we're just timing the logic that underpins the
    // retrieval of no value.

    @TimeStep(prob = 0)
    public String remove(ThreadState state) {
        return map.remove(state.randomKey());
    }

    @TimeStep(prob = 0)
    public void delete(ThreadState state) {
        map.delete(state.randomKey());
    }

    @TimeStep(prob = 0)
    public boolean cas(ThreadState state) {
        // 'v' is always associated with 'k'
        return map.compareAndSet(state.randomKey(), v, v);
    }

    @TimeStep(prob = 0)
    public void createThenDelete(ThreadState state) {
        String key = state.randomKey();
        map.set(key, v);
        map.delete(key);
    }

    public class ThreadState extends BaseThreadState {
        public String randomKey() {
            return keys[randomInt(distinctKeys)];
        }
    }
}