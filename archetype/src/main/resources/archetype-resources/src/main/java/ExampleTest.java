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
package ${package};

import com.hazelcast.core.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;


import static org.junit.Assert.assertEquals;

public class ExampleTest extends HazelcastTest {

    // properties
    public int maxKeys = 1000;

    private IMap<Integer, String> map;

    @Setup
    public void setUp() {
        this.map = targetInstance.getMap(name);
    }

    @Prepare
    public void prepare() {
        logger.info("Map size is: " + map.size());
    }

    @TimeStep(prob = 0.5)
    public void put(BaseThreadState state) {
        int key = state.randomInt(maxKeys);
        map.put(key, "value" + key);
    }

    @TimeStep(prob = 0.5)
    public void get(BaseThreadState state) {
        int key = state.randomInt(maxKeys);
        map.get(key);
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

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
