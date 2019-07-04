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
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateAsciiStrings;

public class StringStringMapTest extends HazelcastTest {

    // properties
    public int keyCount = 10000;
    public int valueCount = 10000;
    public int keyLength = 10;
    public int valueLength = 10;
    public int minValueLength = valueLength;
    public int maxValueLength = valueLength;
    public KeyLocality keyLocality = KeyLocality.SHARED;
    public int minNumberOfMembers = 0;

    private IMap<String, String> map;
    private String[] keys;
    private String[] values;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
    }

    @Prepare
    public void prepare() {
        waitClusterSize(logger, targetInstance, minNumberOfMembers);
        keys = generateStringKeys(keyCount, keyLength, keyLocality, targetInstance);
        values = generateAsciiStrings(valueCount, minValueLength, maxValueLength);

        loadInitialData();
    }

    private void loadInitialData() {
        Random random = new Random();
        Streamer<String, String> streamer = StreamerFactory.getInstance(map);
        for (String key : keys) {
            String value = values[random.nextInt(valueCount)];
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @TimeStep(prob = -1)
    public void get(ThreadState state) {
        String key = state.randomKey();
        map.get(key);
    }

    @TimeStep(prob = 0.1)
    public void put(ThreadState state) {
        String key = state.randomKey();
        String value = state.randomValue();
        map.put(key, value);
    }

    @TimeStep(prob = 0)
    public void set(ThreadState state) {
        String key = state.randomKey();
        String value = state.randomValue();
        map.set(key, value);
    }

    public class ThreadState extends BaseThreadState {

        private String randomKey() {
            return keys[randomInt(keys.length)];
        }

        private String randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
