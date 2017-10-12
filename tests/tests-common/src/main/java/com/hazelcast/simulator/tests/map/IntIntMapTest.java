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

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;

public class IntIntMapTest extends AbstractTest {

    // properties
    public int keyCount = 10000;
    public KeyLocality keyLocality = KeyLocality.SHARED;
    public int minNumberOfMembers = 0;
    public boolean useSet = false;

    private IMap<Integer, Integer> map;
    private int[] keys;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
    }

    @Prepare(global = false)
    public void prepare() {
        waitClusterSize(logger, targetInstance, minNumberOfMembers);
        keys = generateIntKeys(keyCount, keyLocality, targetInstance);
        Streamer<Integer, Integer> streamer = StreamerFactory.getInstance(map);
        Random random = new Random();
        for (int key : keys) {
            int value = random.nextInt(Integer.MAX_VALUE);
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @TimeStep(prob = 0.1)
    public void put(ThreadState state) {
        int key = state.randomKey();
        int value = state.randomValue();

        // TODO: a future cleanup is to pull the set() call into its own timeStep method
        if (useSet) {
            map.set(key, value);
        } else {
            map.put(key, value);
        }
    }

    @TimeStep(prob = -1)
    public void get(ThreadState state) {
        int key = state.randomKey();
        map.get(key);
    }

    public class ThreadState extends BaseThreadState {

        private int randomKey() {
            return keys[randomInt(keys.length)];
        }

        private int randomValue() {
            return randomInt(Integer.MAX_VALUE);
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
        logger.info(getOperationCountInformation(targetInstance));
    }
}
