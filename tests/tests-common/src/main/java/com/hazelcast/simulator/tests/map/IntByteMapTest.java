/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;

public class IntByteMapTest extends AbstractTest {

    // properties
    public int keyCount = 1000;
    public int valueCount = 1000;
    public int minSize = 16;
    public int maxSize = 2000;
    public KeyLocality keyLocality = KeyLocality.SHARED;

    private IMap<Integer, Object> map;
    private int[] keys;
    private byte[][] values;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        keys = generateIntKeys(keyCount, keyLocality, targetInstance);

        if (minSize > maxSize) {
            throw new IllegalStateException("minSize can't be larger than maxSize");
        }
    }

    @Prepare
    public void prepare() {
        Random random = new Random();
        values = new byte[valueCount][];
        for (int i = 0; i < values.length; i++) {
            int delta = maxSize - minSize;
            int length = delta == 0 ? minSize : minSize + random.nextInt(delta);
            values[i] = generateByteArray(random, length);
        }

        Streamer<Integer, Object> streamer = StreamerFactory.getInstance(map);
        for (int key : keys) {
            streamer.pushEntry(key, values[random.nextInt(values.length)]);
        }
        streamer.await();
    }

    @TimeStep(prob = 0.1)
    public void put(ThreadState state) {
        map.put(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0.0)
    public void set(ThreadState state) {
        map.set(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = -1)
    public void get(ThreadState state) {
        map.get(state.randomKey());
    }

    public class ThreadState extends BaseThreadState {

        private int randomKey() {
            return keys[randomInt(keys.length)];
        }

        private byte[] randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
