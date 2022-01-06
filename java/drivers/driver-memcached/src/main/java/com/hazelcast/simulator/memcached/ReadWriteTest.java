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
package com.hazelcast.simulator.memcached;

import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.SerializingTranscoder;

import java.util.Random;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateAsciiStrings;

public class ReadWriteTest extends MemcachedTest {

    // properties
    public int keyCount = 1000;
    public int keyLength = 10;
    public int valueCount = 1000;
    public int minSize = 16;
    public int maxSize = 2000;
    public int ttl = 0; // by default insert immortal entries
    // the number of keys that are going to be written.
    // normally you want to keep this the same as keyCount (for reading), but it can help to expose certain problems like
    // gc. If they writeKeyCount is very small, only a small group of objects get updated frequently and helps to prevent
    // getting them tenured. If writeKeyCount is -1, it will automatically be set to keyCount
    public int writeKeyCount = -1;

    private String[] keys;
    private byte[][] values;

    @Setup
    public void setUp() {
        keys = generateAsciiStrings(keyCount, keyLength);

        if (minSize > maxSize) {
            throw new IllegalStateException("minSize can't be larger than maxSize");
        }

        if (writeKeyCount == -1) {
            writeKeyCount = keyCount;
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

        for (String key : keys) {
            client.add(key, ttl, values[random.nextInt(values.length)]);
        }
    }

    @TimeStep(prob = 0.1)
    public OperationFuture<Boolean> put(ThreadState state) {
        return client.add(state.randomKey(), ttl, state.randomValue(), state.transcoder());
    }

    @TimeStep(prob = 0.0)
    public boolean putWithCheck(ThreadState state) throws Exception {
        return client.add(state.randomKey(), ttl, state.randomValue(), state.transcoder()).get();
    }

    @TimeStep(prob = 0.0)
    public OperationFuture<Boolean> set(ThreadState state) {
        return client.set(state.randomWriteKey(), ttl, state.randomValue(), state.transcoder());
    }

    @TimeStep(prob = 0.0)
    public boolean setWithCheck(ThreadState state) throws Exception {
        return client.set(state.randomWriteKey(), ttl, state.randomValue(), state.transcoder()).get();
    }

    @TimeStep(prob = -1)
    public Object get(ThreadState state) {
        return client.get(state.randomKey());
    }

    public class ThreadState extends BaseThreadState {

        private SerializingTranscoder transcoder;

        private SerializingTranscoder transcoder() {
            if (transcoder == null) {
                transcoder = new SerializingTranscoder(Integer.MAX_VALUE);
                transcoder.setCompressionThreshold(Integer.MAX_VALUE);
            }

            return transcoder;
        }

        private String randomKey() {
            return keys[randomInt(keys.length)];
        }

        private String randomWriteKey() {
            return keys[randomInt(writeKeyCount)];
        }

        private byte[] randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown
    public void tearDown() {
        client.shutdown();
    }
}
