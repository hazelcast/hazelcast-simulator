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

import com.hazelcast.core.Pipelining;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.util.concurrent.Executor;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateAsciiStrings;

 public class MetricsCarouselTest extends HazelcastTest {

    // properties
    public int keyDomain = 10000;
    public int valueCount = 10000;
    public int minValueLength = 10;
    public int maxValueLength = 10;

    private String[] values;

    @Setup
    public void setUp() {
        values = generateAsciiStrings(valueCount, minValueLength, maxValueLength);
    }

    @Prepare(global = true)
    public void prepare() {

    }

    @TimeStep(prob = 1)
    public void createMapsAndDeleteThemLater(ThreadState state) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            String name = state.randomMapName();
            IMap<Integer, Integer> map = targetInstance.getMap(name);
            map.put(state.randomInt(), state.randomInt());
            Thread.sleep(60_000);
            map.destroy();
        }
    }

    public class ThreadState extends BaseThreadState {
        private String randomMapName() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown
    public void tearDown() {
    }
}
