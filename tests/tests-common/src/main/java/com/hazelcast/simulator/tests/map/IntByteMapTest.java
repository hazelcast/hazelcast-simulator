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
import com.hazelcast.simulator.test.BaseThreadContext;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.AbstractTest;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.KeyLocality.SHARED;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;

public class IntByteMapTest extends AbstractTest {

    // properties
    public int keyCount = 1000;
    public int valueCount = 1000;
    public int minSize = 16;
    public int maxSize = 2000;
    public KeyLocality keyLocality = SHARED;

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

    @Warmup
    public void warmup() {
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
    public void put(BaseThreadContext context) {
        int key = keys[context.randomInt(keys.length)];
        byte[] value = values[context.randomInt(values.length)];
        map.put(key, value);
    }

    @TimeStep(prob = 0)
    public void set(BaseThreadContext context) {
        int key = keys[context.randomInt(keys.length)];
        byte[] value = values[context.randomInt(values.length)];
        map.set(key, value);
    }

    @TimeStep(prob = 0.9)
    public void get(BaseThreadContext context) {
        int key = keys[context.randomInt(keys.length)];
        map.get(key);
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
