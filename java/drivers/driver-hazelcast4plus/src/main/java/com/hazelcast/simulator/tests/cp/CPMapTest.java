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
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.cp.helpers.CpMapOperationCounter;
import com.hazelcast.simulator.utils.GeneratorUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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

    private List<CPMap<Integer, byte[]>> mapReferences;

    private byte[][] values;

    private IList<CpMapOperationCounter> operationCounterList;

    @Setup
    public void setup() {
        values = createValues();

        // (1) create the cp group names that will host the maps
        String[] cpGroupNames = createCpGroupNames();
        // (2) create the map names + associated proxies (maps aren't created until you actually interface with them)
        mapReferences = new ArrayList<>();
        for (int i = 0; i < maps; i++) {
            String cpGroup = cpGroupNames[i % cpGroupNames.length];
            String mapName = "map" + i + "@" + cpGroup;
            mapReferences.add(targetInstance.getCPSubsystem().getMap(mapName));
        }

        operationCounterList = targetInstance.getList(name + "Report");
    }

    private byte[][] createValues() {
        byte[][] valuesArray = new byte[valuesCount][valueSizeBytes];
        Random random = new Random(0);
        for (int i = 0; i < valuesArray.length; i++) {
            valuesArray[i] = GeneratorUtils.generateByteArray(random, valueSizeBytes);
        }
        return valuesArray;
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

    @TimeStep(prob = 1)
    public void set(ThreadState state) {
        state.randomMap().set(state.randomKey(), state.randomValue());
        state.operationCounter.setCount++;
    }

    @TimeStep(prob = 0)
    public void put(ThreadState state) {
        state.randomMap().put(state.randomKey(), state.randomValue());
        state.operationCounter.putCount++;
    }

    @TimeStep(prob = 0)
    public void putIfAbsent(ThreadState state) {
        state.randomMap().putIfAbsent(state.randomKey(), state.randomValue());
        state.operationCounter.putIfAbsentCount++;
    }

    @TimeStep(prob = 0)
    public void get(ThreadState state) {
        state.randomMap().get(state.randomKey());
        state.operationCounter.getCount++;
    }

    // 'remove' and 'delete' other than their first invocation pointless -- we're just timing the logic that underpins the
    // retrieval of no value.

    @TimeStep(prob = 0)
    public void remove(ThreadState state) {
        state.randomMap().remove(state.randomKey());
        state.operationCounter.removeCount++;
    }

    @TimeStep(prob = 0)
    public void delete(ThreadState state) {
        state.randomMap().delete(state.randomKey());
        state.operationCounter.deleteCount++;
    }

    @TimeStep(prob = 0)
    public void cas(ThreadState state) {
        CPMap<Integer, byte[]> randomMap = state.randomMap();
        Integer key = state.randomKey();
        byte[] expectedValue = randomMap.get(key);
        if (expectedValue != null) {
            randomMap.compareAndSet(key, expectedValue, state.randomValue());
            state.operationCounter.casCount++;
        }
    }

    @TimeStep(prob = 0)
    public void setThenDelete(ThreadState state) {
        CPMap<Integer, byte[]> map = state.randomMap();
        int key = state.randomKey();
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
        for (CPMap<Integer, byte[]> mapReference : mapReferences) {
            int entriesCount = 0;
            for (int key = 0; key < keys; key++) {
                byte[] get = mapReference.get(key);
                if (get != null) {
                    entriesCount++;
                }
            }
            // Just check that CP map after test contains any item.
            // In theory we can deliberately remove all keys but this is not expected way how we want to use this test.
            assertTrue("CP Map " + mapReference.getName() + " doesn't contain any of expected items.", entriesCount > 0);
        }
    }

    public class ThreadState extends BaseThreadState {

        final CpMapOperationCounter operationCounter = new CpMapOperationCounter();

        public int randomKey() {
            return randomInt(keys); // [0, keys)
        }

        public byte[] randomValue() {
            return values[randomInt(valuesCount)]; // [0, values)
        }

        public CPMap<Integer, byte[]> randomMap() {
            return mapReferences.get(randomInt(maps));
        }
    }
}