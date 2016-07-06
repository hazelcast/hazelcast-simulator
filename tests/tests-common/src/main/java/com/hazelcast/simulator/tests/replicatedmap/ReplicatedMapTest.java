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
package com.hazelcast.simulator.tests.replicatedmap;

import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.simulator.test.BaseThreadContext;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.AbstractTest;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateStrings;

public class ReplicatedMapTest extends AbstractTest {

    // properties
    public int keyCount = 10000;
    public int valueCount = 1;
    public int valueLength = 10;

    private ReplicatedMap<Integer, String> map;

    private String[] values;

    @Setup
    public void setUp() throws Exception {
        map = targetInstance.getReplicatedMap(basename + "-" + testContext.getTestId());
    }

    @Warmup
    public void warmup() throws InterruptedException {
        values = generateStrings(valueCount, valueLength);
    }

    @TimeStep(prob = 0.45)
    public void put(ThreadContext context) {
        int key = context.randomInt(keyCount);
        String value = context.randomValue();
        map.put(key, value);
    }

    @TimeStep(prob = 0.45)
    public void get(ThreadContext context) {
        int key = context.randomInt(keyCount);
        map.get(key);
    }

    @TimeStep(prob = 0.1)
    public void remove(ThreadContext context) {
        int key = context.randomInt(keyCount);
        map.remove(key);
    }

    public class ThreadContext extends BaseThreadContext {
        private String randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown
    public void tearDown() throws Exception {
        map.destroy();
    }
}
