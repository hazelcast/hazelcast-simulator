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
package com.hazelcast.simulator.tests.special;

import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isClient;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.nextKeyOwnedBy;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.FormatUtils.humanReadableByteCount;
import static java.lang.String.format;

/**
 * Fills up the cluster to a defined heap usage factor during prepare phase.
 *
 * This tests intentionally has an empty run phase.
 */
public class MapHeapHogPrepareTest extends AbstractTest {

    private static final long APPROX_ENTRY_BYTES_SIZE = 238;

    // properties
    public int threadCount = 10;
    public int logFrequency = 50000;
    public int ttlHours = 24;
    public double approxHeapUsageFactor = 0.9;

    private IMap<Long, Long> map;

    private long maxEntriesPerThread;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);

        // calculate how many entries we have to insert per member and thread
        long free = Runtime.getRuntime().freeMemory();
        long total = Runtime.getRuntime().totalMemory();
        long max = Runtime.getRuntime().maxMemory();

        long used = total - free;
        long totalFree = max - used;

        long maxLocalEntries = (long) ((totalFree / (double) APPROX_ENTRY_BYTES_SIZE) * approxHeapUsageFactor);
        maxEntriesPerThread = (long) (maxLocalEntries / (double) threadCount);
    }

    @Prepare
    public void prepare() {
        if (isClient(targetInstance)) {
            return;
        }

        // fill the cluster as fast as possible with data
        long startKey = 0;
        boolean isLogger = true;
        ThreadSpawner threadSpawner = new ThreadSpawner(name);
        for (int i = 0; i < threadCount; i++) {
            threadSpawner.spawn(new FillMapWorker(isLogger, startKey));
            isLogger = false;
            startKey += maxEntriesPerThread;
        }
        threadSpawner.awaitCompletion();

        StringBuilder sb = new StringBuilder(name).append(": After prepare phase the map size is ").append(map.size());
        addMemoryStatistics(sb);
        logger.info(sb.toString());
    }

    @TimeStep
    public void timeStep() {
        testContext.stop();
    }

    private final class FillMapWorker implements Runnable {

        private final boolean isLogger;

        private long key;

        private FillMapWorker(boolean isLogger, long startKey) {
            this.isLogger = isLogger;
            this.key = startKey;
        }

        @Override
        public void run() {
            StringBuilder sb = new StringBuilder();
            for (int workerIteration = 0; workerIteration < maxEntriesPerThread; workerIteration++) {
                key = nextKeyOwnedBy(targetInstance, key);
                map.put(key, key, ttlHours, TimeUnit.HOURS);
                key++;

                if (isLogger && key % logFrequency == 0) {
                    sb.append(name).append(": In prepare phase the map size is ").append(map.size());
                    addMemoryStatistics(sb);
                    logger.info(sb.toString());
                    sb.setLength(0);
                }
            }
        }
    }

    private static void addMemoryStatistics(StringBuilder sb) {
        long free = Runtime.getRuntime().freeMemory();
        long total = Runtime.getRuntime().totalMemory();
        long max = Runtime.getRuntime().maxMemory();

        long used = total - free;
        long totalFree = max - used;
        double usedOfMax = 100.0 * ((double) used / (double) max);

        sb.append(NEW_LINE).append("free = ").append(humanReadableByteCount(free, true)).append(" (").append(free).append(')')
                .append(NEW_LINE).append("total free = ").append(humanReadableByteCount(totalFree, true))
                .append(" (").append(totalFree).append(')')
                .append(NEW_LINE).append("used = ").append(humanReadableByteCount(used, true))
                .append(" (").append(used).append(')')
                .append(NEW_LINE).append("max = ").append(humanReadableByteCount(max, true)).append(" (").append(max).append(')')
                .append(NEW_LINE).append(format("usedOfMax = %.2f%%", usedOfMax));
    }

    @Verify
    public void verify() {
        if (isClient(targetInstance)) {
            return;
        }

        StringBuilder sb = new StringBuilder(name).append(": In local verify phase the map size is ").append(map.size());
        addMemoryStatistics(sb);
        logger.info(sb.toString());
    }
}
