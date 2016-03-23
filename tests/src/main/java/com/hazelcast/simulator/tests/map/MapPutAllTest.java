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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.GenericTypes;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;

/**
 * Test for {@link IMap#putAll(Map)} which uses a set of prepared maps with input values during the RUN phase.
 *
 * You can configure the {@link #keyType} and {@link #valueType} for the used maps.
 */
public class MapPutAllTest {

    private static final ILogger LOGGER = Logger.getLogger(MapPutAllTest.class);

    // properties
    public String basename = MapPutAllTest.class.getSimpleName();

    public KeyLocality keyLocality = KeyLocality.SHARED;
    public GenericTypes keyType = GenericTypes.STRING;
    public GenericTypes valueType = GenericTypes.STRING;
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
    // if we want to use putAll() or put() in a loop (this is a nice setting to see what kind of speedup or slowdown to expect)
    public boolean usePutAll = true;

    private HazelcastInstance targetInstance;
    private IMap<Object, Object> map;
    private Map<Object, Object>[] inputMaps;

    @Setup
    public void setUp(TestContext testContext) {
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
    }

    @Teardown
    public void tearDown() {
        map.destroy();
        LOGGER.info(getOperationCountInformation(targetInstance));

        if (valueType == GenericTypes.INTEGER) {
            valueSize = Integer.MAX_VALUE;
        }
    }

    @Warmup(global = false)
    @SuppressWarnings("unchecked")
    public void warmup() {
        Object[] keys = keyType.generateKeys(targetInstance, keyLocality, keyCount, keySize);

        inputMaps = new Map[mapCount];
        Random random = new Random();
        for (int mapIndex = 0; mapIndex < mapCount; mapIndex++) {
            // generate a SortedMap or HashMap depending on the configuration
            Map<Object, Object> tmpMap = (useSortedMap ? new TreeMap<Object, Object>() : new HashMap<Object, Object>(itemCount));
            for (int itemIndex = 0; itemIndex < itemCount; itemIndex++) {
                Object key = keys[random.nextInt(keyCount)];
                Object value = valueType.generateValue(random, valueSize);
                tmpMap.put(key, value);
            }
            inputMaps[mapIndex] = tmpMap;
        }
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        @Override
        protected void timeStep() throws Exception {
            Map<Object, Object> insertMap = randomMap();
            if (usePutAll) {
                map.putAll(insertMap);
            } else {
                for (Map.Entry<Object, Object> entry : insertMap.entrySet()) {
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }

        private Map<Object, Object> randomMap() {
            return inputMaps[randomInt(inputMaps.length)];
        }
    }

    public static void main(String[] args) throws Exception {
        MapPutAllTest test = new MapPutAllTest();
        new TestRunner<MapPutAllTest>(test).withDuration(10).run();
    }
}
