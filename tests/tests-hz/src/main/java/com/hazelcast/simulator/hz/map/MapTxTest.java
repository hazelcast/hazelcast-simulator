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
package com.hazelcast.simulator.hz.map;

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
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.KeyLocality.SHARED;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;

public class MapTxTest extends HazelcastTest {

    // properties
    public int keyCount = 10000;
    public int valueCount = 1000;
    public KeyLocality keyLocality = SHARED;
    public int minSize = 16;
    public int maxSize = 2000;

    private IMap<Integer, byte[]> map;
    private int[] keys;
    private byte[][] values;

    @Setup
    public void setup() {
        map = targetInstance.getMap(name);
    }

    @Prepare
    public void prepare() {
        keys = generateIntKeys(keyCount, keyLocality, targetInstance);

        Random random = new Random();
        values = new byte[valueCount][];
        for (int i = 0; i < values.length; i++) {
            int delta = maxSize - minSize;
            int length = delta == 0 ? minSize : minSize + random.nextInt(delta);
            values[i] = generateByteArray(random, length);
        }

        Streamer<Integer, byte[]> streamer = StreamerFactory.getInstance(map);
        for (int key : keys) {
            byte[] value = values[random.nextInt(valueCount)];
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @TimeStep(prob = 0)
    public Object getForUpdatePut(final ThreadState state) {
        return targetInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext ctx) {
                int key = state.randomKey();
                byte[] value = state.randomValue();
                TransactionalMap<Integer, byte[]> txMap = ctx.getMap(name);
                txMap.getForUpdate(key);
                return txMap.put(key, value);
            }
        });
    }

    @TimeStep(prob = 0)
    public Object getForUpdateSet(final ThreadState state) {
        return targetInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext ctx) {
                int key = state.randomKey();
                byte[] value = state.randomValue();
                TransactionalMap<Integer, byte[]> txMap = ctx.getMap(name);
                txMap.getForUpdate(key);
                txMap.set(key, value);
                return null;
            }
        });
    }

    @TimeStep(prob = 0.1)
    public Object put(final ThreadState state) {
         return targetInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext ctx) {
                int key = state.randomKey();
                byte[] value = state.randomValue();
                TransactionalMap<Integer, byte[]> txMap = ctx.getMap(name);
                return txMap.put(key, value);
            }
        });
    }

    @TimeStep(prob = 0)
    public Object set(final ThreadState state) {
        return targetInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext ctx) {
                int key = state.randomKey();
                byte[] value = state.randomValue();
                TransactionalMap<Integer, byte[]> txMap = ctx.getMap(name);
                txMap.set(key, value);
                return 0;
            }
        });
    }

    @TimeStep(prob = -1)
    public Object get(final ThreadState state) {
        final int key = state.randomKey();
        return targetInstance.executeTransaction(new TransactionalTask<Object>() {
            @Override
            public Object execute(TransactionalTaskContext ctx) {
                TransactionalMap<Integer, byte[]> txMap = ctx.getMap(name);
                return txMap.get(key);
            }
        });
    }

    public class ThreadState extends BaseThreadState {

        private int randomKey() {
            int length = keys.length;
            return keys[randomInt(length)];
        }

        private byte[] randomValue() {
            return values[randomInt(valueCount)];
        }
    }

    @Teardown
    public void teardown() {
        map.destroy();
    }
}
