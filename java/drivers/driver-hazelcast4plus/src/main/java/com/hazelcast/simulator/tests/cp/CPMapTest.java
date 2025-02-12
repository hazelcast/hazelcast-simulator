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
import com.hazelcast.cp.CPMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.cp.helpers.CpMapOperationCounter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static org.junit.Assert.assertTrue;

/**
 * This test is running as part of release verification simulator test. Hence every change in this class should be
 * discussed with QE team since it can affect release verification tests.
 */
public class CPMapTest extends HazelcastTest {
    // number of cp groups to host the created maps; if (distinctMaps % maps) == 0 then there's a uniform distribution of maps
    // over cp groups, otherwise maps are allocated per-cp group in a RR-fashion. If cpGroups == 0 then all CPMap instances will
    // be hosted by the default CPGroup. When cpGroups > 0, we create and host CPMaps across non-default CP Groups.
    public int cpGroups = 0;
    // number of distinct maps to create and use during the tests
    public int maps = 1;
    // number of distinct keys to create and use per-map; key domain is [0, keys)
    public int keys = 1;
    // number of possible values
    public int valuesCount = 100;
    // size in bytes for each key's associated value
    public int valueSizeBytes = 100;
    public int keySizeBytes = 100;
    private byte[] val;

    private List<CPMap<String,  byte[]>> mapReferences;

    private byte[][] values;
    private List<String> keyPool;

    private IList<CpMapOperationCounter> operationCounterList;
    public boolean preload = false;

    @Setup
    public void setup() {
        val = new byte[valueSizeBytes];
        keyPool = new ArrayList<>();

        logger.info("Generating " + keys + " keys");
        for (int i = 0; i < keys; i++) {
            keyPool.add(generateString(i));
        }
        logger.info("Generated " + keys + " keys");
        // print all keys
        for (int i = 0; i < keys; i++) {
            logger.info("Key: " + keyPool.get(i));
        }

        // (1) create the cp group names that will host the maps
        String[] cpGroupNames = createCpGroupNames();
        // (2) create the map names + associated proxies (maps aren't created until you actually interface with them)
        mapReferences = new ArrayList<>();
        for (int i = 0; i < maps; i++) {
            String cpGroup = cpGroupNames[i % cpGroupNames.length];
            String mapName = "map" + i + "@" + cpGroup;
            logger.info("Creating CPMap: " + mapName);
            mapReferences.add(targetInstance.getCPSubsystem().getMap(mapName));
        }

        operationCounterList = targetInstance.getList(name + "Report");
        logger.info("Sleeping for 60s to allow CP Subsystem to stabilize");
        sleepSeconds(60);
    }

    @Prepare(global = true)
    public void prepare() {
        if (!preload) {
            logger.info("Checking map already contains keys");
            for (CPMap<String, byte[]> mapReference : mapReferences) {
                // just check for first, middle and last key
                for (int key : new int[]{0, keys / 2, keys - 1}) {
                    byte[] get = mapReference.get(keyPool.get(key));
                    if (get != null) {
                        logger.info("Map " + mapReference.getName() + " already contains key: " + keyPool.get(key));
                    } else {
                        logger.info("Map " + mapReference.getName() + " doesn't contain key: " + keyPool.get(key));
                        throw new IllegalStateException("Map " + mapReference.getName() + " doesn't contain key: " + keyPool.get(key));
                    }
                }
            }
            logger.info("Waiting for latencies to be applied ... ");
            sleepSeconds(60);

            return;
        }
        // Determine the number of threads based on available processors
        int threadCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        // Atomic counter to track the total number of keys set
        AtomicInteger totalKeysSet = new AtomicInteger(0);

        // Calculate batch size based on the total number of keys and the number of threads
        int batchSize = (int) Math.ceil((double) keyPool.size() / threadCount);

        // Ensure each thread gets a unique portion of the keyPool
        for (int t = 0; t < threadCount; t++) {
            // Define the range of keys for this thread
            int start = t * batchSize;
            int end = Math.min(start + batchSize, keyPool.size());

            // Create a sublist of keys for this thread, ensuring unique keys per thread
            List<String> keyBatch = new ArrayList<>(keyPool.subList(start, end));

            // Submit the batch processing task to the executor
            executor.submit(() -> {
                int i = 0;
                for (String key : keyBatch) {
                    // Add each key-value pair to the map
                    mapReferences.get(0).set(key, val);

                    // Increment the atomic counter
                    totalKeysSet.incrementAndGet();

                    // Log progress every 10,000 keys
                    if (i++ % 10000 == 0) {
                        logger.info("Preloaded " + i + " keys in this batch.");
                    }
                }
            });
        }

        executor.shutdown();

        try {
            // Wait until all tasks are finished, effectively waiting indefinitely
            while (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                // Optional: log or monitor progress every minute while waiting
                logger.info("Waiting for all threads to complete...");
            }
        } catch (InterruptedException e) {
            // Handle interruptions properly; this will rarely happen
            Thread.currentThread().interrupt();
        }

        // Log the final count of keys set
        logger.info("Total number of keys set: " + totalKeysSet.get());
    }


    private String[] createCpGroupNames() {
        if (cpGroups == 0) {
            return new String[]{"default"};
        }

        String[] cpGroupNames = new String[cpGroups];
        for (int i = 0; i < cpGroups; i++) {
            cpGroupNames[i] = "cpgroup-" + i;
        }
        return cpGroupNames;
    }

    @TimeStep(prob = 0)
    public void set(ThreadState state) {
        state.getNextMap().set(keyPool.get(state.randomKey()), state.randomValue());
        state.operationCounter.setCount++;
    }

    @TimeStep(prob = 0)
    public void put(ThreadState state) {
        state.getNextMap().put(keyPool.get(state.randomKey()), state.randomValue());
        state.operationCounter.putCount++;
    }

    @TimeStep(prob = 0)
    public void putIfAbsent(ThreadState state) {
        state.getNextMap().putIfAbsent(keyPool.get(state.randomKey()), val);
        state.operationCounter.putIfAbsentCount++;
    }

    @TimeStep(prob = 0)
    public void get(ThreadState state) {
        state.getNextMap().get(keyPool.get(state.randomKey()));
        state.operationCounter.getCount++;
    }

    // 'remove' and 'delete' other than their first invocation pointless -- we're just timing the logic that underpins the
    // retrieval of no value.

    @TimeStep(prob = 0)
    public void remove(ThreadState state) {
        state.getNextMap().remove(keyPool.get(state.randomKey()));
        state.operationCounter.removeCount++;
    }

    @TimeStep(prob = 0)
    public void delete(ThreadState state) {
        state.getNextMap().delete(keyPool.get(state.randomKey()));
        state.operationCounter.deleteCount++;
    }

    @TimeStep(prob = 0)
    public void cas(ThreadState state) {
        CPMap<String,  byte[]> randomMap = state.getNextMap();
        String key = keyPool.get(state.randomKey());
        byte[] expectedValue = randomMap.get(key);
        if (expectedValue != null) {
            randomMap.compareAndSet(key, expectedValue, state.randomValue());
            state.operationCounter.casCount++;
        }
    }

    @TimeStep(prob = 0)
    public void setThenDelete(ThreadState state) {
        CPMap<String, byte[]> map = state.getNextMap();
        String key = keyPool.get(state.randomKey());
        map.set(key, state.randomValue());
        map.delete(key);
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        operationCounterList.add(state.operationCounter);
    }

    @Verify(global = true)
    public void verify() {
        // print stats
        CpMapOperationCounter total = new CpMapOperationCounter();
        for (CpMapOperationCounter operationCounter : operationCounterList) {
            total.add(operationCounter);
        }
        logger.info(name + ": " + total + " from " + operationCounterList.size() + " worker threads");

        // basic verification
        for (CPMap<String, byte[]> mapReference : mapReferences) {
            int entriesCount = 0;
            for (int key = 0; key < keys; key++) {
                byte[] get = mapReference.get(keyPool.get(key));
                if (get != null) {
                    entriesCount++;
                }
            }
            // Just check that CP map after test contains any item.
            // In theory we can deliberately remove all keys but this is not expected way how we want to use this test.
            logger.info(name + ":  CP Map " + mapReference.getName() + " entries count: " + entriesCount);
            assertTrue("CP Map " + mapReference.getName() + " doesn't contain any of expected items.", entriesCount > 0);
        }
    }

    public class ThreadState extends BaseThreadState {

        private int currentMapIndex = 0;
        final CpMapOperationCounter operationCounter = new CpMapOperationCounter();

        public int randomKey() {
            return randomInt(keys); // [0, keys)
        }

        public byte[] randomValue() {
            return values[randomInt(valuesCount)]; // [0, values)
        }

        public CPMap<String, byte[]> getNextMap() {
            if (currentMapIndex == maps) {
                currentMapIndex = 0;
            }
            return mapReferences.get(currentMapIndex++);
        }
    }

    private String generateString(int number) {
        String prefix = "PREFIX_";

        String numberStr = Integer.toString(number);

        // Calculate the number of characters needed to fill up the string
        int totalCharacters = keySizeBytes / 2;
        int remainingLength = totalCharacters - (prefix.length() + numberStr.length());

        // Deterministic fixed string for each input number
        StringBuilder fixedDigits = new StringBuilder(remainingLength);

        // Use a fixed pattern
        String pattern = Integer.toString(number).repeat((remainingLength / numberStr.length()) + 1);

        // Append up to remainingLength characters from the pattern
        fixedDigits.append(pattern.substring(0, remainingLength));

        String result = prefix + numberStr + fixedDigits;

        // Ensure the result is exactly the desired number of characters
        if (result.length() != totalCharacters) {
            throw new IllegalStateException("Generated string is not exactly the correct length.");
        }

        return result;
    }
}