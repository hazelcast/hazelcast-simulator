/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.map.nearcache;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.internal.util.collection.LongHashSet;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.assignKeyToIndex;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArrays;
import static java.lang.Thread.currentThread;

public class NearCacheLongByteArrayMapTest extends HazelcastTest {

    private static final Function<Long, Object> ANY_OWNER_FN = key -> 1;

    // properties
    public int keyDomain = 10000;
    public int valueCount = 10000;
    public int minValueLength = 10;
    public int maxValueLength = 10;
    public int batchSize = 100;
    public int mapCount = 1;

    /**
     * Enables use of get instead of getAll
     */
    public boolean iterative;

    private byte[][] values;
    private final List<List<IMap<Long, byte[]>>> maps = new ArrayList<>();
    private final Executor callerRuns = Runnable::run;
    private final Random random = new Random();

    // Tracks which thread is assigned which client by its index
    private final Map<Thread, Integer> clientIndexForThread = new ConcurrentHashMap<>();

    private IExecutorService executor;
    private IExecutorService noopExecutor;
    private Function<Long, Member> keyToOwnerFn;

    @Setup
    public void setUp() {
        keyToOwnerFn = key -> targetInstance.getPartitionService().getPartition(key).getOwner();

        executor = targetInstance.getExecutorService("nc-exec");
        noopExecutor = targetInstance.getExecutorService("noop-exec");

        for (HazelcastInstance instance : getTargetInstances()) {
            List<IMap<Long, byte[]>> mapsForInstance = new ArrayList<>();
            maps.add(mapsForInstance);
            for (int i = 0; i < mapCount; i++) {
                String mapName = (mapCount == 1) ? name : name + "_" + i;
                mapsForInstance.add(instance.getMap(mapName));
            }
        }
        values = generateByteArrays(valueCount, minValueLength, maxValueLength);
    }

    @Prepare(global = true)
    public void prepare() {
        // We only need to use one instance to prepare the maps
        for (IMap<Long, byte[]> map : maps.get(0)) {
            Streamer<Long, byte[]> streamer = StreamerFactory.getInstance(map);
            for (long key = 0; key < keyDomain; key++) {
                byte[] value = values[random.nextInt(valueCount)];
                streamer.pushEntry(key, value);
            }
            streamer.await();
        }
    }

    private IMap<Long, byte[]> getRandomMap() {
        List<IMap<Long, byte[]>> mapsToSelectFrom;
        if (maps.size() == 1) {
            mapsToSelectFrom = maps.get(0);
        } else {
            Integer clientIndex = clientIndexForThread.get(currentThread());
            mapsToSelectFrom = maps.get(clientIndex == null ? putClientForCurrentThread() : clientIndex);
        }
        return mapsToSelectFrom.get(random.nextInt(mapCount));
    }

    private synchronized int putClientForCurrentThread() {
        return assignKeyToIndex(getTargetInstances().size(), currentThread(), clientIndexForThread);
    }

    /**
     * {@link #populate(ThreadState)} and {@link #invalidate(ThreadState)} form a sequential scenario
     */
    @TimeStep(prob = 0)
    public void populate(ThreadState state) throws ExecutionException, InterruptedException {
        var map = getRandomMap();
        long key = state.newRandomCurrentKey();
        var result = executor.submitToKeyOwner(new PopulateNearCacheForKey(map.getName(), key), key).get();
        if (!result) {
            throw new IllegalStateException("key does not exists in IMap");
        }
    }

    @TimeStep(prob = 0)
    public void populateBatch(ThreadState state) throws ExecutionException, InterruptedException {
        var map = getRandomMap();

        var keys = state.newRandomCurrentKeyBatch(batchSize, keyToOwnerFn);
        var result = executor.submitToKeyOwner(new PopulateNearCacheForKey(map.getName(), keys).iterative(iterative), keys[0]).get();
        if (!result) {
            throw new IllegalStateException("key does not exists in IMap");
        }
    }

    @TimeStep(prob = 0)
    public void populateBatchAnyOwner(ThreadState state) throws ExecutionException, InterruptedException {
        var map = getRandomMap();

        var keys = state.newRandomCurrentKeyBatch(batchSize, ANY_OWNER_FN);
        var result = executor.submitToKeyOwner(new PopulateNearCacheForKey(map.getName(), keys).iterative(iterative), keys[0]).get();
        if (!result) {
            throw new IllegalStateException("key does not exists in IMap");
        }
    }

    @TimeStep(prob = 0)
    public void invalidate(ThreadState state) {
        var map = getRandomMap();
        long key = state.currentKey();
        // we populate NC only on owner, so (in theory!!!) it should be cleared here without waiting for the batch
        // TODO: but will it be cleared again when the batch hits?
        map.set(key, state.randomValue());
    }

    @TimeStep(prob = 0)
    public void invalidateViaProxy(ThreadState state) throws ExecutionException, InterruptedException {
        var map = getRandomMap();
        long key = state.currentKey();

        var result = executor.submitToKeyOwner(new InvalidateNearCacheForKey(map.getName(), key, state.randomValue()), key).get();
    }

    @TimeStep(prob = 0)
    public void invalidateViaProxyBatch(ThreadState state) throws ExecutionException, InterruptedException {
        var map = getRandomMap();
        var keys = state.currentKeysBatch();
        if (keys == null) {
            // not initialized yet, will be next time
            return;
        }

        var result = executor.submitToKeyOwner(new InvalidateNearCacheForKey(map.getName(), keys, state.randomValue()), keys[0]).get();
    }

    // control
    @TimeStep(prob = 0)
    public void noop(ThreadState state) throws ExecutionException, InterruptedException {
        var map = getRandomMap();
        long key = state.currentKey();

        noopExecutor.submitToKeyOwner(new NoopCallableForKey(map.getName(), key), key).get();
    }

    // control
    @TimeStep(prob = 0)
    public byte[] get(ThreadState state) {
        return getRandomMap().get(state.randomKey());
    }

    @TimeStep(prob = 0)
    public void updateAllUsingEntryProcessor() {
        IMap<Long, byte[]> map = getRandomMap();
        map.executeOnEntries(new UpdateEntryProcessor((byte) 1));
    }

    public class ThreadState extends BaseThreadState {
        private long currentKey = randomKey();

        private long[] currentKeys;
        private LongHashSet seenKeys;

        private long randomKey() {
            return randomLong(keyDomain);
        }

        private byte[] randomValue() {
            return values[randomInt(values.length)];
        }

        public long currentKey() {
            return currentKey;
        }

        public long newRandomCurrentKey() {
            return currentKey = randomKey();
        }

        long[] newRandomCurrentKeyBatch(int batchSize, Function<Long, ?> keyToOwnerFn) {
            if (currentKeys == null || currentKeys.length != batchSize) {
                currentKeys = new long[batchSize];
                seenKeys = new LongHashSet(batchSize, -1);
            }
            currentKeys[0] = randomKey();
            var owner = keyToOwnerFn.apply(currentKeys[0]);
            seenKeys.clear();
            seenKeys.add(currentKeys[0]);
            for (int i = 1; i < batchSize; ++i) {
                do {
                    currentKeys[i] = randomKey();
                } while (!owner.equals(keyToOwnerFn.apply(currentKeys[i]))
                        // exclude duplicates
                        || !seenKeys.add(currentKeys[i]));
            }
            return currentKeys;
        }

        long[] currentKeysBatch() {
            return currentKeys;
        }
    }

    private static final class UpdateEntryProcessor implements EntryProcessor<Long, byte[], Object> {

        private final byte increment;

        private UpdateEntryProcessor(byte increment) {
            this.increment = increment;
        }

        @Override
        public Object process(Map.Entry<Long, byte[]> entry) {
            byte[] value = entry.getValue();
            value[0] += increment;
            entry.setValue(value);
            return null;
        }
    }

    @Teardown
    public void tearDown() {
        maps.stream().flatMap(Collection::stream).forEach(IMap::destroy);
    }
}
