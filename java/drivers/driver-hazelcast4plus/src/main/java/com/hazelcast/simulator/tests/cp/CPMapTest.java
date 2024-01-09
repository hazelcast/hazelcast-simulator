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
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.GeneratorUtils;

import java.util.UUID;

/**
 * Note that these tests are designed to be run in isolation w.r.t. one another.
 */
public class CPMapTest extends HazelcastTest {
    public int keySizeBytes = 128;
    public int valueSizeBytes = 1024;
    private CPMap<String, String> map;

    private String k;
    private String v; // this is always the value associated with 'k'; exception is remove and delete


    @Setup
    public void setup() {
        k = createString(keySizeBytes);
        v = createString(valueSizeBytes);
        map = targetInstance.getCPSubsystem().getMap("map");
        map.set(k, v);
    }

    String createString(int bytes) {
        // as it's ascii
        return GeneratorUtils.generateAsciiString(bytes);
    }

    @TimeStep(prob = 1)
    public void set(ThreadState state) {
        map.set(k, v);
    }

    @TimeStep(prob = 0)
    public String put(ThreadState state) {
        return map.put(k, v);
    }

    @TimeStep(prob = 0)
    public String get(ThreadState state) {
        return map.get(k);
    }

    // 'remove' and 'delete' other than their first invocation pointless -- we're just timing the logic that underpins the
    // retrieval of no value.

    @TimeStep(prob = 0)
    public String remove(ThreadState state) {
        return map.remove(k);
    }

    @TimeStep(prob = 0)
    public void delete(ThreadState state) {
        map.delete(k);
    }

    @TimeStep(prob = 0)
    public boolean cas(ThreadState state) {
        // 'v' is always associated with 'k'
        return map.compareAndSet(k, v, v);
    }

    @TimeStep(prob = 0)
    public void createThenDelete(ThreadState state) {
        String key = UUID.randomUUID().toString();
        map.set(key, v);
        map.delete(key);
    }

    public class ThreadState extends BaseThreadState {
    }
}