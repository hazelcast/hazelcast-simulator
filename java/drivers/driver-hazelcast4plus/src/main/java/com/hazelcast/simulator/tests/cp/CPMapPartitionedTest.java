/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.cp;

import com.hazelcast.collection.IList;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.cp.helpers.CPMapPartitioned;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertNotNull;

/**
 * Generates load against a {@link CPMapPartitioned}, sharded across {@code partitionCount} CP groups.
 */
public class CPMapPartitionedTest extends HazelcastTest {

    private static final Integer VALUE = 0;

    // size of the key domain; keys are the ids [0, keyCount)
    public int keyCount = 1_000_000;
    // total length in characters/bytes of each generated key (a zero-padded decimal index)
    public int keySize = 32;
    // number of CP groups to shard across. Prime by default: CPMapPartitioned
    // note that 31 is selected as it's < vcpus on a c5.9xlarge, so each CPGroup (should) map-to a distinct
    // operation thread
    public int partitionCount = 31;
    // number of threads used to parallelize the keyspace preload
    public int preloadThreads = 8;
    // how often (in milliseconds) the preload emits a progress log line, useful for large loads
    public long preloadProgressLogIntervalMs = 15_000;

    private Function<String, Integer> getter;
    private BiConsumer<String, Integer> setter;
    private BiFunction<String, Integer, Integer> putIfAbsenter;
    private IList<long[]> operationCounts;

    private String keyForIndex(int index) {
        return String.format("%0" + keySize + "d", index);
    }

    private static boolean isPrime(int n) {
        if (n < 2) {
            return false;
        }
        for (int i = 2; (long) i * i <= n; i++) {
            if (n % i == 0) {
                return false;
            }
        }
        return true;
    }

    @Setup
    public void setup() {
        if (keyCount <= 0) {
            throw new IllegalArgumentException("keyCount must be > 0, was " + keyCount);
        }
        int minKeySize = Integer.toString(keyCount - 1).length();
        if (keySize < minKeySize) {
            throw new IllegalArgumentException("keySize must be >= " + minKeySize
                    + " digits to represent keyCount " + keyCount + ", was " + keySize);
        }

        if (!isPrime(partitionCount)) {
            logger.warn(name + ": partitionCount " + partitionCount + " is not prime; a prime "
                    + "partitionCount is recommended for a more even key distribution");
        }
        CPMapPartitioned<String, Integer> partitioned = new CPMapPartitioned<>(targetInstance, name, partitionCount);
        getter = partitioned::get;
        setter = partitioned::set;
        putIfAbsenter = partitioned::putIfAbsent;

        operationCounts = targetInstance.getList(name + "Report");
    }

    // note: this is used to bound the storage before the test runs so we remove that variable
    // for snapshotting it means that we're generally communicating a 'full' snapshot per snapshot 
    // event rather than some intermediate size.
    @Prepare(global = true)
    public void prepare() {
        AtomicLong preloadedCount = new AtomicLong();
        AtomicLong nextLogAtMillis = new AtomicLong(System.currentTimeMillis() + preloadProgressLogIntervalMs);

        ThreadSpawner spawner = new ThreadSpawner(name);
        int shardSize = (keyCount + preloadThreads - 1) / preloadThreads;
        for (int t = 0; t < preloadThreads; t++) {
            int start = t * shardSize;
            int end = Math.min(start + shardSize, keyCount);
            spawner.spawn(() -> {
                for (int i = start; i < end; i++) {
                    setter.accept(keyForIndex(i), VALUE);
                    long count = preloadedCount.incrementAndGet();

                    long logAt = nextLogAtMillis.get();
                    long now = System.currentTimeMillis();
                    if (now >= logAt && nextLogAtMillis.compareAndSet(logAt, now + preloadProgressLogIntervalMs)) {
                        logger.info(name + ": key loaded: " + count + " / " + keyCount);
                    }
                }
            });
        }
        spawner.awaitCompletion();

        logger.info(name + ": preloaded " + keyCount + " keys of " + keySize + " bytes each");
    }

    @TimeStep(prob = 0.5)
    public void get(ThreadState state) {
        getter.apply(state.randomKey());
        state.getCount++;
    }

    @TimeStep(prob = 0.5)
    public void set(ThreadState state) {
        setter.accept(state.randomKey(), VALUE);
        state.setCount++;
    }

    @TimeStep(prob = 0)
    public void putIfAbsent(ThreadState state) {
        putIfAbsenter.apply(state.randomKey(), VALUE);
        state.putIfAbsentCount++;
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        operationCounts.add(new long[]{state.getCount, state.setCount, state.putIfAbsentCount});
    }

    @Verify(global = true)
    public void verify() {
        long totalGets = 0;
        long totalSets = 0;
        long totalPutIfAbsents = 0;
        for (long[] counts : operationCounts) {
            totalGets += counts[0];
            totalSets += counts[1];
            totalPutIfAbsents += counts[2];
        }
        logger.info(name + ": totalGets=" + totalGets + " totalSets=" + totalSets
                + " totalPutIfAbsents=" + totalPutIfAbsents
                + " from " + operationCounts.size() + " worker threads");

        // sanity-check a handful of sample keys
        int[] sampleIndexes = {0, keyCount / 2, keyCount - 1};
        for (int index : sampleIndexes) {
            Integer value = getter.apply(keyForIndex(index));
            assertNotNull(name + ": expected preloaded key at index " + index + " to be present", value);
        }
    }

    public class ThreadState extends BaseThreadState {
        long getCount;
        long setCount;
        long putIfAbsentCount;

        String randomKey() {
            return keyForIndex(randomInt(keyCount));
        }
    }
}
