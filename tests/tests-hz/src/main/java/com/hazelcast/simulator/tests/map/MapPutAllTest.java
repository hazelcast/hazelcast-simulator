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

import com.hazelcast.core.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.helpers.GenericTypes;
import com.hazelcast.simulator.tests.helpers.KeyLocality;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static com.hazelcast.simulator.tests.helpers.GenericTypes.STRING;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.KeyLocality.SHARED;

/**
 * Test for {@link IMap#putAll(Map)} which uses a set of prepared maps with input values during the RUN phase.
 *
 * You can configure the {@link #keyType} and {@link #valueType} for the used maps.
 */
public class MapPutAllTest extends HazelcastTest {

    // properties
    public KeyLocality keyLocality = SHARED;
    public GenericTypes keyType = STRING;
    public GenericTypes valueType = STRING;
    public int keyCount = 1000000;
    public int itemCount = 10000;

    // the length of the key (just used with keyType STRING)
    public int keySize = 10;
    // the length of the the value (just used with keyType STRING)
    public int valueSize = 100;
    // the number of prepared maps with input values (we don't want to keep inserting the same values over an over again)
    public int mapCount = 2;

    // if we want to used a SortedMap for input values
    public boolean useSortedMap = true;

    private IMap<Object, Object> map;
    private Map<Object, Object>[] inputMaps;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
    }

    @Prepare
    @SuppressWarnings("unchecked")
    public void prepare() {
        Object[] keys = keyType.generateKeys(targetInstance, keyLocality, keyCount, keySize);

        inputMaps = new Map[mapCount];
        Random random = new Random();
        for (int mapIndex = 0; mapIndex < mapCount; mapIndex++) {
            // generate a SortedMap or HashMap depending on the configuration
            Map<Object, Object> tmpMap = (useSortedMap ? new TreeMap<Object, Object>() : new HashMap<Object, Object>(itemCount));
            while (tmpMap.size() < itemCount) {
                Object key = keys[random.nextInt(keyCount)];
                Object value = valueType.generateValue(random, valueSize);
                tmpMap.put(key, value);
            }
            inputMaps[mapIndex] = tmpMap;
        }
    }

    @TimeStep(prob = 1)
    public void putAll(ThreadState state) {
        Map<Object, Object> insertMap = state.randomMap();
        map.putAll(insertMap);
    }

    @TimeStep(prob = 0)
    public void put(ThreadState state) {
        Map<Object, Object> insertMap = state.randomMap();
        for (Map.Entry<Object, Object> entry : insertMap.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
    }

    public class ThreadState extends BaseThreadState {

        private Map<Object, Object> randomMap() {
            return inputMaps[randomInt(inputMaps.length)];
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
        logger.info(getOperationCountInformation(targetInstance));

        if (valueType == GenericTypes.INTEGER) {
            valueSize = Integer.MAX_VALUE;
        }
    }
}
