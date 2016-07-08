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
package com.hazelcast.simulator.tests;

import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.BaseThreadContext;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;

import static org.junit.Assert.assertEquals;

public class ExampleTest extends AbstractTest {

    // properties
    public int maxKeys = 1000;
    public double putProb = 0.5;

    private IMap<Integer, String> map;

    @Setup
    public void setUp() {
        map = targetInstance.getMap("exampleMap");
        logger.info("Map name is: " + map.getName());
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }

    @Warmup
    public void warmup() {
        logger.info("Map size is: " + map.size());
    }

    @Verify
    public void verify() {
        logger.info("Map size is: " + map.size());

        for (int i = 0; i < maxKeys; i++) {
            String actualValue = map.get(i);
            if (actualValue != null) {
                String expectedValue = "value" + i;
                assertEquals(expectedValue, actualValue);
            }
        }
    }

    @TimeStep
    public void put(BaseThreadContext context) {
        int key = context.randomInt(maxKeys);
        map.put(key, "value" + key);
    }

    @TimeStep
    public void get(BaseThreadContext context) {
        int key = context.randomInt(maxKeys);
        map.get(key);
    }
}
