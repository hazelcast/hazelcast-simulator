/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.external;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.probes.Probe;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static java.lang.String.format;
import static org.junit.Assert.fail;

public final class ExternalClientUtils {

    private static final int SET_COUNT_DOWN_LATCH_RETRIES = 5;
    private static final int EXPECTED_RESULTS_MAX_RETRIES = 60;

    private static final ILogger LOGGER = Logger.getLogger(ExternalClientUtils.class);

    private ExternalClientUtils() {
    }

    public static void setCountDownLatch(ICountDownLatch countDownLatch, int value) {
        if (value < 1) {
            fail("Value for CountDownLatch must be positive, but was: " + value);
        }
        for (int i = 0; i <= SET_COUNT_DOWN_LATCH_RETRIES; i++) {
            if (countDownLatch.trySetCount(value)) {
                break;
            }
            sleepMillis(100);
        }
        if (countDownLatch.getCount() != value) {
            LOGGER.severe("Could not set ICountDownLatch value of " + value);
            fail("Could not set ICountDownLatch value of " + value);
        }
    }

    public static void getThroughputResults(HazelcastInstance hazelcastInstance, int expectedResultSize) {
        IList<String> throughputResults = getResultList(hazelcastInstance, "externalClientsThroughputResults",
                expectedResultSize);
        int resultSize = throughputResults.size();

        LOGGER.info(format("Collecting %d throughput results (expected %d)...", resultSize, expectedResultSize));
        int totalInvocations = 0;
        double totalDuration = 0;
        for (String throughputString : throughputResults) {
            String[] throughput = throughputString.split("\\|");
            int operationCount = Integer.parseInt(throughput[0]);
            long duration = TimeUnit.NANOSECONDS.toMillis(Long.parseLong(throughput[1]));

            String publisherId = "n/a";
            if (throughput.length > 2) {
                publisherId = throughput[2];
            }
            LOGGER.info(format("External client executed %d operations in %d ms (%s)", operationCount, duration, publisherId));

            totalInvocations += operationCount;
            totalDuration += duration;
        }
        LOGGER.info("Done!");

        if (resultSize == 0 || totalInvocations == 0 || totalDuration == 0) {
            LOGGER.info(format("No valid throughput probe data collected! results: %d, totalInvocations: %d, totalDuration: %.0f",
                    resultSize, totalInvocations, totalDuration));
            return;
        }

        long avgDuration = Math.round(totalDuration / resultSize);
        double performance = ((double) totalInvocations / avgDuration) * 1000;
        LOGGER.info(format("All external clients executed %d operations in %d ms (%.3f ops/s)",
                totalInvocations, avgDuration, performance));
    }

    public static void getLatencyResults(HazelcastInstance hazelcastInstance, Probe probe, int expectedResultSize) {
        IList<String> latencyLists = getResultList(hazelcastInstance, "externalClientsLatencyResults", expectedResultSize);

        LOGGER.info(format("Collecting %d latency result lists...", latencyLists.size()));
        for (String key : latencyLists) {
            IList<Long> values = hazelcastInstance.getList(key);
            LOGGER.info(format("Adding %d latency results...", values.size()));
            for (Long latency : values) {
                probe.recordValue(latency);
            }
        }
        LOGGER.info("Done!");
    }

    public static IList<String> getResultList(HazelcastInstance hazelcastInstance, String listName, int expectedResultSize) {
        int lastSize = 0;
        int retries = 0;
        IList<String> resultList = hazelcastInstance.getList(listName);

        // wait for all throughput results to arrive
        while (expectedResultSize > 0) {
            int listSize = resultList.size();

            // check if we should stop
            if (listSize >= expectedResultSize) {
                break;
            }

            // check if there is progress
            if (lastSize == listSize) {
                retries++;
            } else {
                retries = 0;
            }
            if (retries > EXPECTED_RESULTS_MAX_RETRIES) {
                break;
            }
            lastSize = listSize;

            LOGGER.info(format("Waiting for %d/%d results of %s...", resultList.size(), expectedResultSize, listName));
            sleepSeconds(1);
        }
        return resultList;
    }
}
