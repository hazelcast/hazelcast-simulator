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

import com.hazelcast.core.Pipelining;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.probes.LatencyProbe;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.StartNanos;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.HotSetKeySelector;
import com.hazelcast.simulator.utils.KeySelector;
import com.hazelcast.simulator.utils.RandomKeySelector;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArrays;

public class LongByteArrayMapTest extends HazelcastTest {

    // properties
    public int keyDomain = 10000;
    public int valueCount = 10000;
    public int minValueLength = 10;
    public int maxValueLength = 10;
    public int pipelineDepth = 10;
    public int pipelineIterations = 100;
    public int getAllSize = 5;
    public String selector = "random";
    public int hotSetPercentage = 10;
    public int hotSetAccessPercentage = 90;
    private KeySelector keySelector;

    private IMap<Long, byte[]> map;
    private byte[][] values;
    private final Executor callerRuns = Runnable::run;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        values = generateByteArrays(valueCount, minValueLength, maxValueLength);
        if ("hotset".equalsIgnoreCase(selector)) {
            keySelector = new HotSetKeySelector(0, keyDomain, hotSetAccessPercentage, hotSetPercentage);
        } else {
            keySelector = new RandomKeySelector(keyDomain);
        }
        logger.info("Using " + keySelector);
    }

    @Prepare(global = true)
    public void prepare() {
        Random random = new Random();
        Streamer<Long, byte[]> streamer = StreamerFactory.getInstance(map);
        for (long key = 0; key < keyDomain; key++) {
            byte[] value = values[random.nextInt(valueCount)];
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @TimeStep(prob = -1)
    public byte[] get(ThreadState state) {
        return map.get(state.randomKey());
    }

    @TimeStep(prob = -1)
    public Map<Long, byte[]> getAll(ThreadState state) {
        Set<Long> keys = new HashSet<>();
        for (int k = 0; k < getAllSize; k++) {
            keys.add(state.randomKey());
        }
        return map.getAll(keys);
    }

    @TimeStep(prob = 0)
    public CompletableFuture getAsync(ThreadState state) {
        return map.getAsync(state.randomKey()).toCompletableFuture();
    }

    @TimeStep(prob = 0.1)
    public byte[] put(ThreadState state) {
        return map.put(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0.0)
    public CompletableFuture putAsync(ThreadState state) {
        return map.putAsync(state.randomKey(), state.randomValue()).toCompletableFuture();
    }

    @TimeStep(prob = 0)
    public void set(ThreadState state) {
        map.set(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0)
    public CompletableFuture setAsync(ThreadState state) {
        return map.setAsync(state.randomKey(), state.randomValue()).toCompletableFuture();
    }

    @TimeStep(prob = 0)
    public void pipelinedGet(final ThreadState state, @StartNanos final long startNanos, final LatencyProbe probe) throws Exception {
        if (state.pipeline == null) {
            state.pipeline = new Pipelining<>(pipelineDepth);
        }

        CompletableFuture<byte[]> f = map.getAsync(state.randomKey()).toCompletableFuture();
        f.whenCompleteAsync((bytes, throwable) -> probe.done(startNanos), callerRuns);
        state.pipeline.add(f);
        state.i++;
        if (state.i == pipelineIterations) {
            state.i = 0;
            state.pipeline.results();
            state.pipeline = null;
        }
    }

    public class ThreadState extends BaseThreadState {
        private Pipelining<byte[]> pipeline;
        private int i;

        private long randomKey() {
            return keySelector.nextKey(this::randomLong);
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
