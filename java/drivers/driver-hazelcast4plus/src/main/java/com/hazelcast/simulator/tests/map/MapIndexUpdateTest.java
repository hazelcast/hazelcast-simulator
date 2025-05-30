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

import com.hazelcast.config.IndexType;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * Benchmark of updates of IMap with many indexes and/or TTL. Heavily uses IMap RecordStore (entry updates),
 * attribute extraction, index updates (B+Tree in case of HD) and allocators (HD index) or GC (on-heap index),
 * expiry metadata.
 */
public class MapIndexUpdateTest extends HazelcastTest {

    // properties
    public int keyDomain = 10000;
    public int deskCodeCount = 100;

    public int timeVarianceMs = 10_000;
    public boolean addIndexes = true;
    public boolean longIndex = false;

    public int getAllSize = 5;
    public int mapCount = 1;

    private final List<IMap<Integer, Trade>> maps = new ArrayList<>();
    private final Random random = new Random();

    private String[] deskCodes;

    @Setup
    public void setUp() {
        deskCodes = IntStream.range(0, deskCodeCount)
                .mapToObj(i -> i == 0 ? null : String.format("DESK_%d", i))
                .toArray(String[]::new);

        for (int i = 0; i < mapCount; i++) {
            String mapName = (mapCount == 1) ? name : name + "_" + i;
            IMap<Integer, Trade> map = targetInstance.getMap(mapName);
            maps.add(map);
            if (addIndexes) {
                map.addIndex(IndexType.SORTED, "epochSeconds");
                map.addIndex(IndexType.HASH, "deskCode");
                map.addIndex(IndexType.SORTED, "id1");
                if (longIndex) {
                    map.addIndex(IndexType.SORTED, "id2");
                }
                map.addIndex(IndexType.HASH, "rnd");
            }
        }
    }

    @Prepare(global = true)
    public void prepare() {
        for (var map : maps) {
            var streamer = StreamerFactory.getInstance(map);
            for (int key = 0; key < keyDomain; key++) {
                streamer.pushEntry(key, randomValue());
            }
            streamer.await();
        }
    }

    private IMap<Integer, Trade> getRandomMap() {
        return maps.get(random.nextInt(mapCount));
    }

    @TimeStep(prob = 0)
    public Trade get(ThreadState state) {
        return getRandomMap().get(state.randomKey());
    }

    @TimeStep(prob = 0)
    public Map<Integer, Trade> getAll(ThreadState state) {
        Set<Integer> keys = new HashSet<>();
        for (int k = 0; k < getAllSize; k++) {
            keys.add(state.randomKey());
        }
        return getRandomMap().getAll(keys);
    }

    @TimeStep(prob = 0)
    public CompletableFuture getAsync(ThreadState state) {
        return getRandomMap().getAsync(state.randomKey()).toCompletableFuture();
    }

    @TimeStep(prob = 0)
    public Trade put(ThreadState state) {
        return getRandomMap().put(state.randomKey(), randomValue());
    }

    @TimeStep(prob = 0)
    public CompletableFuture deleteAndSetWithTtl(ThreadState state) {
        int key = state.randomKey();
        IMap<Integer, Trade> map = getRandomMap();
        return map.deleteAsync(key).thenComposeAsync(__ ->
                map.setAsync(key, randomValue(), 1, TimeUnit.DAYS)).toCompletableFuture();
    }

    @TimeStep(prob = 0)
    public CompletableFuture putAsync(ThreadState state) {
        return getRandomMap().putAsync(state.randomKey(), randomValue()).toCompletableFuture();
    }

    @TimeStep(prob = 0)
    public void set(ThreadState state) {
        getRandomMap().set(state.randomKey(), randomValue());
    }

    @TimeStep(prob = 0)
    public CompletableFuture setAsync(ThreadState state) {
        return getRandomMap().setAsync(state.randomKey(), randomValue()).toCompletableFuture();
    }

    public class ThreadState extends BaseThreadState {

        private int randomKey() {
            return randomInt(keyDomain);
        }

    }

    private String randomDeskCode() {
        return deskCodes[ThreadLocalRandom.current().nextInt(deskCodes.length)];
    }

    private Trade randomValue() {
        return new Trade((int) ((System.currentTimeMillis() - timeVarianceMs / 2 + ThreadLocalRandom.current().nextInt(timeVarianceMs)) / 1000),
                randomDeskCode(),
                // note: this is not actual id of the entry, but has the same cardinality
                ThreadLocalRandom.current().nextInt(keyDomain));
    }

    @Teardown
    public void tearDown() {
        for (var map : maps) {
            map.destroy();
        }
    }


    public static class Trade implements DataSerializable {
        private int epochSeconds;
        private String deskCode;
        private int id1;
        private int rnd;
        private String id2;

        public Trade() {
        }

        public Trade(int epochSeconds, String deskCode, int id) {
            this.epochSeconds = epochSeconds;
            this.deskCode = deskCode;
            this.id1 = id;
            this.rnd = ThreadLocalRandom.current().nextInt();
            this.id2 = UUID.randomUUID().toString() + UUID.randomUUID() + UUID.randomUUID() + UUID.randomUUID();
        }

        public int getEpochSeconds() {
            return epochSeconds;
        }

        public String getDeskCode() {
            return deskCode;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Trade person = (Trade) o;
            return epochSeconds == person.epochSeconds && Objects.equals(deskCode, person.deskCode);
        }

        @Override
        public int hashCode() {
            return Objects.hash(epochSeconds, deskCode);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(epochSeconds);
            out.writeString(deskCode);
            out.writeInt(id1);
            out.writeInt(rnd);
            out.writeString(id2);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            this.epochSeconds = in.readInt();
            this.deskCode = in.readString();
            this.id1 = in.readInt();
            this.rnd = in.readInt();
            this.id2 = in.readString();
        }
    }
}
