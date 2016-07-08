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

public class IntIntMapTest extends AbstractTest {

    // properties
    public int keyCount = 10000;
    public int valueCount = 10000;
    public int keyLength = 10;
    public int valueLength = 10;
    public KeyLocality keyLocality = SHARED;

    private IMap<Integer, Integer> map;
    private int[] keys;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }

    @Warmup
    public void warmUp() {
        keys = generateIntKeys(keyCount, keyLocality, targetInstance);
        Streamer<Integer, Integer> streamer = StreamerFactory.getInstance(map);
        Random random = new Random();
        for (int key : keys) {
            int value = random.nextInt(Integer.MAX_VALUE);
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @TimeStep
    public void get(ThreadContext context) {
        map.get(context.randomKey());
    }

    @TimeStep
    public void put(ThreadContext context) {
        map.put(context.randomKey(), context.randomValue());
    }

    @TimeStep
    public void set(ThreadContext context) {
        map.set(context.randomKey(), context.randomValue());
    }

    public class ThreadContext extends BaseThreadContext {

        private int randomKey() {
            return keys[randomInt(keys.length)];
        }

        private int randomValue() {
            return randomInt(Integer.MAX_VALUE);
        }
    }
}
