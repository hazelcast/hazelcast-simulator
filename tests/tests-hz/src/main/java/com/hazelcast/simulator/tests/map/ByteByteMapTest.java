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
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Random;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;

public class ByteByteMapTest extends HazelcastTest {

    // properties
    public int keyCount = 1000;
    public int keyLength = 10;
    public int valueCount = 1000;
    public int valueSize = 1000;

    private IMap<Object, Object> map;
    private byte[][] keys;
    private byte[][] values;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        Random random = new Random();
        keys = new byte[keyCount][];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = generateByteArray(random, keyLength);
        }
    }

    @Prepare
    public void prepare() {
        Random random = new Random();
        values = new byte[valueCount][];
        for (int i = 0; i < values.length; i++) {
            values[i] = generateByteArray(random, valueSize);
        }

        Streamer<Object, Object> streamer = StreamerFactory.getInstance(map);
        for (byte[] key : keys) {
            streamer.pushEntry(key, values[random.nextInt(values.length)]);
        }
        streamer.await();
    }

    @TimeStep(prob = 0.1)
    public Object put(ThreadState state) {
        return map.put(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = -1)
    public Object get(ThreadState state) {
        return map.get(state.randomKey());
    }

    public class ThreadState extends BaseThreadState {

        private byte[] randomKey() {
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
