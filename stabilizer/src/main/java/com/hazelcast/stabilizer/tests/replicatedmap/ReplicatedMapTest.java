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

package com.hazelcast.stabilizer.tests.replicatedmap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicatedMapTest {

    private static final ILogger log = Logger.getLogger(ReplicatedMapTest.class);

    private static final int OPERATE_PUT = 1;
    private static final int OPERATE_GET = 2;
    private static final int OPERATE_UPDATE = 3;
    private static final int OPERATE_REMOVE = 4;

    // properties.
    public String basename = "replicatedmap";
    public int threadCount = 20;
    public int minBatchSize = 1000;
    public int maxBatchSize = 100000;
    public int putRatio = 50;
    public int getRatio = 100 - putRatio;
    public int updateRatio = 30;
    public int removeRatio = 20;
    public int minClearIntervalSeconds = 3600;
    public int maxClearIntervalSeconds = 86400;
    public int minValueSize = 1024;
    public int maxValueSize = 1024 * 1024;
    public int minSleepBetweenBatchesMillis = 0;
    public int maxSleepBetweenBatchesMillis = 60000;
    public int ttlRatio = 50;
    public int minTtlTimeSeconds = 0;
    public int maxTtlTimeSeconds = 60;
    public int logFrequency = 100;

    private final AtomicLong operations = new AtomicLong(0);
    private ReplicatedMap<String, byte[]> replicatedMap;
    private TestContext testContext;
    private HazelcastInstance hz;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        this.hz = testContext.getTargetInstance();
        replicatedMap = hz.getReplicatedMap(basename + "-" + testContext.getTestId());
    }

    @Warmup
    public void warmup() {
        // Pre-populate 10 elements per connection
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            String key = createKey();
            byte[] value = createValue(random);
            replicatedMap.put(key, value);
        }
    }

    @Run
    public void createTestThreads() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker(k));
        }
        spawner.awaitCompletion();
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private static void log(String message, Object... params) {
        String msg = String.format(message, params);
        log.info(msg);
    }

    private String createKey() {
        return UUID.randomUUID().toString();
    }

    private int randomizeValueSize(Random random) {
        return minValueSize + random.nextInt(maxValueSize - minValueSize);
    }

    private byte[] createValue(Random random) {
        int valueSize = randomizeValueSize(random);
        byte[] value = new byte[valueSize];
        for (int i = 0; i < valueSize; i++) {
            if (i < 5) {
                value[i] = (byte) i;
            } else if ((value.length - i) < 5) {
                value[i] = (byte) (value.length - i);
            } else {
                value[i] = (byte) random.nextInt(255);
            }
        }
        return value;
    }

    private final class Worker
            implements Runnable {

        private final Random random = new Random();
        private final int workerId;

        private Worker(int workerId) {
            this.workerId = workerId;
        }

        @Override
        public void run() {
            try {
                long clearIntervalNanos = randomizeClearInterval();
                long iteration = 0;
                long lastClearTime = System.nanoTime();
                while (!testContext.isStopped()) {
                    long batchSleepNanos = randomizeBatchSleep();
                    int batchSize = randomizeBatchSize();

                    for (int i = 0; i < batchSize; i++) {
                        int operation = randomizeOperation();
                        switch (operation) {
                            case OPERATE_GET:
                                operateGet();
                                break;
                            case OPERATE_PUT:
                                operatePut();
                                break;
                            case OPERATE_UPDATE:
                                operateUpdate();
                                break;
                            case OPERATE_REMOVE:
                                operateRemove();
                                break;
                        }
                        operations.incrementAndGet();

                        if (testContext.isStopped()) {
                            return;
                        }

                        if (iteration % logFrequency == 0) {
                            log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                        }

                        iteration++;
                    }

                    if (System.nanoTime() - lastClearTime >= clearIntervalNanos) {
                        log("Executing clear operation");
                        replicatedMap.clear();
                    }
                    TimeUnit.NANOSECONDS.sleep(batchSleepNanos);
                    if (System.nanoTime() - lastClearTime >= clearIntervalNanos) {
                        log("Executing clear operation");
                        replicatedMap.clear();
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void operateGet() {
            String key = getRandomKey();
            byte[] value = replicatedMap.get(key);
            if (value != null) {
                verifyValue(value);
            }
        }

        private void operatePut() {
            String key = createKey();
            byte[] value = createValue(random);
            int ttl = randomizeTtlSeconds();
            replicatedMap.put(key, value, ttl, TimeUnit.SECONDS);
        }

        private void operateUpdate() {
            String key = getRandomKey();
            byte[] value = createValue(random);
            int ttl = randomizeTtlSeconds();
            replicatedMap.put(key, value, ttl, TimeUnit.SECONDS);
        }

        private void operateRemove() {
            String key = getRandomKey();
            replicatedMap.remove(key);
        }

        private String getRandomKey() {
            List<String> keys = new ArrayList<String>(replicatedMap.keySet());
            int index = random.nextInt(keys.size());
            return keys.get(index);
        }

        private int randomizeOperation() {
            if (!randomizePut()) {
                return OPERATE_GET;
            }

            int rnd = random.nextInt(100);
            if (rnd < updateRatio) {
                return OPERATE_UPDATE;
            } else if (rnd < updateRatio + removeRatio) {
                return OPERATE_REMOVE;
            }
            return OPERATE_PUT;
        }

        private boolean randomizePut() {
            return random.nextInt(100) < putRatio;
        }

        private long randomizeClearInterval() {
            int delta = maxClearIntervalSeconds - minClearIntervalSeconds;
            int interval = minClearIntervalSeconds + random.nextInt(delta);
            return TimeUnit.SECONDS.toNanos(interval);
        }

        private long randomizeBatchSleep() {
            int delta = maxSleepBetweenBatchesMillis - minSleepBetweenBatchesMillis;
            int batchSleep = minSleepBetweenBatchesMillis + random.nextInt(delta);
            return TimeUnit.MILLISECONDS.toNanos(batchSleep);
        }

        private int randomizeBatchSize() {
            return minBatchSize + random.nextInt(maxBatchSize - minBatchSize);
        }

        private int randomizeTtlSeconds() {
            if (random.nextInt(100) > ttlRatio) {
                return 0;
            }
            return minTtlTimeSeconds + random.nextInt(maxTtlTimeSeconds - minTtlTimeSeconds);
        }

        private void verifyValue(byte[] value) {
            for (int i = 0; i < 5; i++) {
                if (i != value[i]) {
                    String message = String.format("First expected: %s but got %s", i, value[i]);
                    log(message);
                    throw new RuntimeException(message);
                }
            }
            for (int i = 1; i < 5; i++) {
                if (i != value[value.length - i]) {
                    String message = String.format("Last expected: %s but got %s", i, value[value.length - i]);
                    log(message);
                    throw new RuntimeException(message);
                }
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        ReplicatedMapTest test = new ReplicatedMapTest();
        new TestRunner(test).withDuration(30).run();
    }
}
