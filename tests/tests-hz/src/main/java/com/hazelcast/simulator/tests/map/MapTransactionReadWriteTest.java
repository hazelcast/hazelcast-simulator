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
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.tests.helpers.KeyLocality.SHARED;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;

public class MapTransactionReadWriteTest extends HazelcastTest {

    // properties
    public int keyCount = 10000;
    public int valueCount = 10000;
    public KeyLocality keyLocality = SHARED;
    public int minNumberOfMembers = 0;

    public boolean useSet = false;

    private IMap<Integer, Integer> map;
    private int[] keys;

    @Setup
    public void setup() {
        map = targetInstance.getMap(name);
    }

    @Prepare
    public void prepare() {
        waitClusterSize(logger, targetInstance, minNumberOfMembers);
        keys = generateIntKeys(keyCount, keyLocality, targetInstance);

        Random random = new Random();
        Streamer<Integer, Integer> streamer = StreamerFactory.getInstance(map);
        for (int key : keys) {
            int value = random.nextInt(Integer.MAX_VALUE);
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @TimeStep(prob = 0.1)
    public void put(ThreadState state) {
        final int key = state.randomKey();
        final int value = state.randomValue();
        targetInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext transactionalTaskContext) {
                TransactionalMap<Integer, Integer> txMap = transactionalTaskContext.getMap(map.getName());
                if (useSet) {
                    txMap.set(key, value);
                } else {
                    txMap.put(key, value);
                }
                return null;
            }
        });
    }

    @TimeStep(prob = -1)
    public void get(ThreadState state) {
        final int key = state.randomKey();
        targetInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext transactionalTaskContext) {
                TransactionalMap<Integer, Integer> txMap = transactionalTaskContext.getMap(map.getName());
                txMap.get(key);
                return null;
            }
        });
    }

    public class ThreadState extends BaseThreadState {

        private int randomKey() {
            int length = keys.length;
            return keys[randomInt(length)];
        }

        private int randomValue() {
            return randomInt(Integer.MAX_VALUE);
        }
    }

    @Teardown
    public void teardown() {
        map.destroy();
        logger.info(getOperationCountInformation(targetInstance));
    }
}
