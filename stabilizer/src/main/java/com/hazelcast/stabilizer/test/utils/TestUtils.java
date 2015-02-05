/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer.test.utils;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import org.apache.log4j.Logger;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.stabilizer.utils.CommonUtils.sleepMillis;
import static com.hazelcast.stabilizer.utils.CommonUtils.sleepMillisThrowException;
import static java.lang.String.format;

public class TestUtils {

    public static final String TEST_INSTANCE = "testInstance";
    public static final int ASSERT_TRUE_EVENTUALLY_TIMEOUT;

    static {
        ASSERT_TRUE_EVENTUALLY_TIMEOUT = Integer.parseInt(System.getProperty(
                "hazelcast.assertTrueEventually.timeout", "300"));
    }

    // we don't want instances
    private TestUtils() {
    }

    public static void warmupPartitions(Logger logger, HazelcastInstance hz) {
        logger.info("Waiting for partition warmup");

        PartitionService partitionService = hz.getPartitionService();
        long startTime = System.currentTimeMillis();
        for (Partition partition : partitionService.getPartitions()) {
            if (System.currentTimeMillis() - startTime > TimeUnit.MINUTES.toMillis(5)) {
                throw new IllegalStateException("Partition warmup timeout. Partitions didn't get an owner in time");
            }

            while (partition.getOwner() == null) {
                logger.debug("Partition owner is not yet set for partitionId: " + partition.getPartitionId());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        logger.info("Partitions are warmed up successfully");
    }

    /**
     * Assert that a certain task is going to assert to true eventually.
     *
     * This method makes use of an exponential back-off mechanism. So initially it will ask frequently, but the
     * more times it fails the less frequent the task is going to be retried.
     *
     * @param task AssertTask to execute
     * @param timeoutSeconds timeout for assert in seconds
     * @throws java.lang.NullPointerException if task is null.
     */
    public static void assertTrueEventually(AssertTask task, long timeoutSeconds) {
        if (task == null) {
            throw new NullPointerException("task can't be null");
        }

        AssertionError error;

        // the total timeout in ms.
        long timeoutMs = TimeUnit.SECONDS.toMillis(timeoutSeconds);

        // the time in ms when the assertTrue is going to expire.
        long expirationMs = System.currentTimeMillis() + timeoutMs;
        int sleepMillis = 100;

        for (;;) {
            try {
                try {
                    task.run();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return;
            } catch (AssertionError e) {
                error = e;
            }

            // there is a timeout, so we are done
            if (System.currentTimeMillis() > expirationMs) {
                throw error;
            }

            sleepMillisThrowException(sleepMillis);
            sleepMillis *= 1.5;
        }
    }

    public static void assertTrueEventually(AssertTask task) {
        assertTrueEventually(task, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static byte[] randomByteArray(Random random, int length) {
        byte[] result = new byte[length];
        random.nextBytes(result);
        return result;
    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }
}
