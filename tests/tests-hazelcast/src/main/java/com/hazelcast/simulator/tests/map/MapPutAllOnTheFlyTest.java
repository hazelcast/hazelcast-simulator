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

import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.KeyLocality.SHARED;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntegerKeys;

/**
 * Test for {@link IMap#putAll(java.util.Map)} which creates the input values on the fly during the RUN phase.
 *
 * You can configure the {@link #batchSize} to determine the number of inserted values per operation.
 * You can configure the {@link #keyRange} to determine the key range for inserted values.
 */
public class MapPutAllOnTheFlyTest extends HazelcastTest {

    // properties
    public int batchSize = 10;
    public int keyRange = 1000000;
    // the number of prepared maps with input values (default is 0 which means everything is created on the fly)
    public int mapCount = 0;

    private Map<Integer, Integer>[] inputMaps;
    private IMap<Integer, Integer> map;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
    }

    @Prepare
    @SuppressWarnings("unchecked")
    public void prepare() {
        if (mapCount > 0) {
            // prepare the input maps
            Integer[] keys = generateIntegerKeys(keyRange, SHARED, targetInstance);

            inputMaps = new Map[mapCount];
            Random random = new Random();
            for (int mapIndex = 0; mapIndex < mapCount; mapIndex++) {
                Map<Integer, Integer> inputMap = new HashMap<>(batchSize);
                while (inputMap.size() < batchSize) {
                    Integer key = keys[random.nextInt(keyRange)];
                    inputMap.put(key, key);
                }
                inputMaps[mapIndex] = inputMap;
            }
        }
    }

    @TimeStep
    public void timeStep(ThreadState state) throws Exception {
        Map<Integer, Integer> inputMap;
        if (mapCount > 0) {
            // use a prepared input map
            inputMap = state.randomMap();
        } else {
            // fill the input map on the fly
            inputMap = state.getMap();
            for (int i = 0; i < batchSize; i++) {
                int key = state.randomInt(keyRange);

                inputMap.put(key, key);
            }
        }

        map.putAll(inputMap);
    }

    public class ThreadState extends BaseThreadState {

        private final Map<Integer, Integer> inputMap = new HashMap<>();

        private Map<Integer, Integer> getMap() {
            inputMap.clear();
            return inputMap;
        }

        private Map<Integer, Integer> randomMap() {
            return inputMaps[randomInt(inputMaps.length)];
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
