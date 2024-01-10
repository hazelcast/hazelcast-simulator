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

import com.hazelcast.cp.CPMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.GeneratorUtils;

import java.util.ArrayList;
import java.util.List;

public class CPMapTest extends HazelcastTest {
    // number of cp groups to host the created maps; if (distinctMaps % maps) == 0 then there's a uniform distribution of maps
    // over cp groups, otherwise maps are allocated per-cp group in a RR-fashion.
    public int cpGroups = 1;
    // number of distinct maps to create and use during the tests
    public int maps = 1;
    // number of distinct keys to create and use per-map; key domain is [0, keys)
    public int keys = 1;
    // size in bytes for each key's associated value
    public int valueSizeBytes = 100;
    private List<CPMap<Integer, String>> mapReferences;

    private String v; // this is always the value associated with any key; exception is remove and delete


    @Setup
    public void setup() {
        v = GeneratorUtils.generateAsciiString(valueSizeBytes);

        // (1) create the cp groups names that will host the maps
        String[] cpGroupNames = new String[cpGroups];
        for (int i = 0; i < cpGroups; i++) {
            cpGroupNames[i] = "cpgroup-" + i;
        }

        // (2) create the map names + associated proxies (maps aren't created until you actually interface with them)
        mapReferences = new ArrayList<>();
        for (int i = 0; i < maps; i++) {
            String cpGroup = cpGroupNames[i % cpGroups];
            String mapName = "map" + i + "@" + cpGroup;
            mapReferences.add(targetInstance.getCPSubsystem().getMap(mapName));
        }
    }

    @Prepare(global = true)
    public void prepare() {
        for (CPMap<Integer, String> mapReference : mapReferences) {
            for (int key = 0; key < keys; key++) {
                mapReference.set(key, v);
            }
        }
    }

    @TimeStep(prob = 1)
    public void set(ThreadState state) {
        state.randomMap().set(state.randomKey(), v);
    }

    @TimeStep(prob = 0)
    public String put(ThreadState state) {
        return state.randomMap().put(state.randomKey(), v);
    }

    @TimeStep(prob = 0)
    public String get(ThreadState state) {
        return state.randomMap().get(state.randomKey());
    }

    // 'remove' and 'delete' other than their first invocation pointless -- we're just timing the logic that underpins the
    // retrieval of no value.

    @TimeStep(prob = 0)
    public String remove(ThreadState state) {
        return state.randomMap().remove(state.randomKey());
    }

    @TimeStep(prob = 0)
    public void delete(ThreadState state) {
        state.randomMap().delete(state.randomKey());
    }

    @TimeStep(prob = 0)
    public boolean cas(ThreadState state) {
        // 'v' is always associated with 'k'
        return state.randomMap().compareAndSet(state.randomKey(), v, v);
    }

    @TimeStep(prob = 0)
    public void createThenDelete(ThreadState state) {
        CPMap<Integer, String> map = state.randomMap();
        int key = state.randomKey();
        map.set(key, v);
        map.delete(key);
    }

    public class ThreadState extends BaseThreadState {
        public int randomKey() {
            return randomInt(keys); // [0, keys)
        }

        public CPMap<Integer, String> randomMap() {
            return mapReferences.get(randomInt(maps));
        }
    }
}