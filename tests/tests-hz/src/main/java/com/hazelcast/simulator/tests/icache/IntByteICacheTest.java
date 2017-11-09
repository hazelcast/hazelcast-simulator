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
package com.hazelcast.simulator.tests.icache;

import com.hazelcast.cache.ICache;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.getCache;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;

public class IntByteICacheTest extends HazelcastTest {

    // properties
    public int keyCount = 1000;
    public int valueCount = 1000;
    public int minSize = 16;
    public int maxSize = 2000;
    // the number of keys that are going to be written.
    // normally you want to keep this the same as keyCount (for reading), but it can help to expose certain problems like
    // gc. If they writeKeyCount is very small, only a small group of objects get updated frequently and helps to prevent
    // getting them tenured. If writeKeyCount is -1, it will automatically be set to keyCount
    public int writeKeyCount = -1;
    public KeyLocality keyLocality = KeyLocality.SHARED;
    // timeout in milliseconds for Cache asynchronous operations
    public int timeoutMs = 50;
    // if true, any occurrence of TimeoutException is caught and only logged, if false it's propagated further
    public boolean catchTimeoutException = false;
    // number of millisecond for stalling the partition thread using stallPartitionThread method
    public int stallTimeMs = 0;

    private final ThrottlingLogger throttlingLogger = ThrottlingLogger.newLogger(logger, 5000);
    private final StallingEntryProcessor stallingEntryProcessor = new StallingEntryProcessor(stallTimeMs);

    private int timeoutExceptionCounter = 0;

    private ICache<Integer, Object> cache;
    private int[] keys;
    private byte[][] values;

    @Setup
    public void setUp() {
        cache = getCache(targetInstance, name);
        keys = generateIntKeys(keyCount, keyLocality, targetInstance);

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

        Streamer<Integer, Object> streamer = StreamerFactory.getInstance(cache);
        for (int key : keys) {
            streamer.pushEntry(key, values[random.nextInt(values.length)]);
        }
        streamer.await();
    }

    @TimeStep(prob = 0.0)
    public void put(ThreadState state) {
        cache.put(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0.1)
    public void putTimed(ThreadState state) throws Exception {
        Future<Void> future = cache.putAsync(state.randomKey(), state.randomValue());
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            timeoutExceptionCounter++;
            if (catchTimeoutException) {
                throttlingLogger.warn("putAsync didn't finish within configured " + timeoutMs + " ms timeout. "
                        + "Number of timeouts occurred: " + timeoutExceptionCounter);
            } else {
                throw ex;
            }
        }
    }

    @TimeStep(prob = 0)
    public void get(ThreadState state) {
        cache.get(state.randomKey());
    }

    @TimeStep(prob = -1)
    public Object getTimed(ThreadState state) throws Exception {
        Future<Object> future = cache.getAsync(state.randomKey());
        Object result = null;
        try {
            result = future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException ex) {
            timeoutExceptionCounter++;
            if (catchTimeoutException) {
                throttlingLogger.warn("getAsync didn't finish within configured " + timeoutMs + " ms timeout. "
                        + "Number of timeouts occurred: " + timeoutExceptionCounter);
            } else {
                throw ex;
            }
        }

        return result;
    }

    @TimeStep(prob = 0)
    public void stall(ThreadState state) {
        cache.invoke(state.randomKey(), stallingEntryProcessor);
    }

    public class ThreadState extends BaseThreadState {

        private int randomKey() {
            return keys[randomInt(keys.length)];
        }

        private byte[] randomValue() {
            return values[randomInt(values.length)];
        }
    }

    static class StallingEntryProcessor implements EntryProcessor<Integer, Object, Object>, Serializable {

        private int stallTime;

        public StallingEntryProcessor(int stallTime) {
            this.stallTime = stallTime;
        }

        @Override
        public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            try {
                Thread.sleep(stallTime);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return entry;
        }
    }

    @Teardown
    public void tearDown() {
        throttlingLogger.info("Total number of TimeoutExceptions occurred: " + timeoutExceptionCounter);
        cache.destroy();
    }
}
