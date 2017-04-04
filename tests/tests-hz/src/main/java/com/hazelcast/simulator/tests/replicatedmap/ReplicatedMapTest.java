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
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateStrings;

public class ReplicatedMapTest extends HazelcastTest {

    // properties
    public int keyCount = 10000;
    public int valueCount = 1;
    public int valueLength = 10;

    private ReplicatedMap<Integer, String> map;

    private String[] values;

    @Setup
    public void setUp() {
        map = targetInstance.getReplicatedMap(name + "-" + testContext.getTestId());
    }

    @Prepare
    public void prepare() {
        values = generateStrings(valueCount, valueLength);
    }

    @TimeStep(prob = 0.45)
    public void put(ThreadState state) {
        int key = state.randomInt(keyCount);
        String value = state.randomValue();
        map.put(key, value);
    }

    @TimeStep(prob = 0.45)
    public void get(ThreadState state) {
        int key = state.randomInt(keyCount);
        map.get(key);
    }

    @TimeStep(prob = -1)
    public void remove(ThreadState state) {
        int key = state.randomInt(keyCount);
        map.remove(key);
    }

    public class ThreadState extends BaseThreadState {
        private String randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
        logger.info(getOperationCountInformation(targetInstance));
    }
}
