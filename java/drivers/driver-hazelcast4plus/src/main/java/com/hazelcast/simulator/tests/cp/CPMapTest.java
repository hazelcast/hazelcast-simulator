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
import com.hazelcast.collection.ISet;
import com.hazelcast.cp.CPMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.cp.helpers.CpMapOperationCounter;
import com.hazelcast.simulator.utils.GeneratorUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
    // number of distinct value to create on every client (total number of values is valuesPerClient * clients)
    public int valuesPerClient = 1;
    // size in bytes for each key's associated value
    public int valueSizeBytes = 100;
    // set it to false if test should not check whether all values have been changed during the test run. Can be useful for short runs
    public boolean checkInitialValueChanged = true;

    private static final String INITIAL_VALUE = "initialValue";
    private List<CPMap<Integer, String>> mapReferences;

    private String v[]; // this is always the value associated with any key; exception is remove and delete

    private IList<CpMapOperationCounter> operationCounterList;
    private ISet<String> allValues;

    @Setup
    public void setup() {
        v = createValues();
        allValues = targetInstance.getSet(name + "AllValues");
        allValues.addAll(Arrays.asList(v));

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

    private String[] createValues() {
        String[] valuesArray = new String[this.valuesPerClient];
        for (int i = 0; i < valuesArray.length; i++) {
            valuesArray[i] = GeneratorUtils.generateAsciiString(valueSizeBytes);
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

    @Prepare(global = true)
    public void prepare() {
        for (CPMap<Integer, String> mapReference : mapReferences) {
            for (int key = 0; key < keys; key++) {
                mapReference.set(key, INITIAL_VALUE);
            }
        }
    }

    @TimeStep(prob = 1)
    public void set(ThreadState state) {
        state.randomMap().set(state.randomKey(), state.randomValue());
        state.operationCounter.setCount.incrementAndGet();
    }

    @TimeStep(prob = 0)
    public void put(ThreadState state) {
        state.randomMap().put(state.randomKey(), state.randomValue());
        state.operationCounter.putCount.incrementAndGet();
    }

    @TimeStep(prob = 0)
    public void get(ThreadState state) {
        state.randomMap().get(state.randomKey());
        state.operationCounter.getCount.incrementAndGet();
    }

    // 'remove' and 'delete' other than their first invocation pointless -- we're just timing the logic that underpins the
    // retrieval of no value.

    @TimeStep(prob = 0)
    public void remove(ThreadState state) {
        state.randomMap().remove(state.randomKey());
        state.operationCounter.removeCount.incrementAndGet();
    }

    @TimeStep(prob = 0)
    public void delete(ThreadState state) {
        state.randomMap().delete(state.randomKey());
        state.operationCounter.deleteCount.incrementAndGet();
    }

    @TimeStep(prob = 0)
    public void cas(ThreadState state) {
        CPMap<Integer, String> randomMap = state.randomMap();
        String expectedValue = randomMap.get(state.randomKey());
        if (expectedValue != null) {
            randomMap.compareAndSet(state.randomKey(), expectedValue, state.randomValue());
            state.operationCounter.casCount.incrementAndGet();
        }
    }

    @TimeStep(prob = 0)
    public void setThenDelete(ThreadState state) {
        CPMap<Integer, String> map = state.randomMap();
        int key = state.randomKey();
        map.set(key, state.randomValue());
        map.delete(key);
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
        if (!checkInitialValueChanged) {
            allValues.add(INITIAL_VALUE);
        }
        for (CPMap<Integer, String> mapReference : mapReferences) {
            int existedKeys = 0;
            for (int key = 0; key < keys; key++) {
                String get = mapReference.get(key);
                if (get != null) {
                    existedKeys++;
                    if (!allValues.contains(get)) {
                        logger.info(name + ": Expected values: ");
                        for (String possibleValue : allValues) {
                            logger.info(name + ": Expected value: " + possibleValue);
                        }
                        logger.info(name + ": Real values: ");
                        for (int i = 0; i < keys; i++) {
                            logger.info(name + ": Real value: " + mapReference.get(i));
                        }
                        assertTrue("Value " + get + " for key " + key + " is unexpected.", allValues.contains(get));
                    }
                }
            }
            // Just check that CP map after test contains any item.
            // In theory we can deliberately remove all keys but this is not expected way how we want to use this test.
            assertTrue("CP Map " + mapReference.getName() + " doesn't contain any of expected items.", existedKeys > 0);
        }
    }

    public class ThreadState extends BaseThreadState {

        final CpMapOperationCounter operationCounter = new CpMapOperationCounter();

        public int randomKey() {
            return randomInt(keys); // [0, keys)
        }

        public String randomValue() {
            return v[randomInt(valuesPerClient)]; // [0, values)
        }

        public CPMap<Integer, String> randomMap() {
            return mapReferences.get(randomInt(maps));
        }
    }
}