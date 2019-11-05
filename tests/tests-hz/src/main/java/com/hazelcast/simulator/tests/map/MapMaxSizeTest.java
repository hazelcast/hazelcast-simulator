/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.map;

import com.hazelcast.collection.IList;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.map.helpers.MapMaxSizeOperationCounter;

import static com.hazelcast.config.MaxSizePolicy.PER_NODE;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static com.hazelcast.simulator.utils.TestUtils.assertEqualsStringFormat;
import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

/**
 * This tests runs {@link IMap#put(Object, Object)} and {@link
 * IMap#get(Object)} operations on a map, which is configured
 * with {@link com.hazelcast.config.MaxSizePolicy#PER_NODE}.
 *
 * With some probability distribution we are doing put, putAsync, get
 * and verification operations on the map. We verify during the test and
 * at the end that the map has not exceeded its max configured size.
 */
public class MapMaxSizeTest extends HazelcastTest {

    // properties
    public int keyCount = Integer.MAX_VALUE;

    private IMap<Object, Object> map;
    private IList<MapMaxSizeOperationCounter> operationCounterList;
    private int maxSizePerNode;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        operationCounterList = targetInstance.getList(name + "OperationCounter");

        if (isMemberNode(targetInstance)) {
            MapConfig mapConfig = targetInstance.getConfig().getMapConfig(name);
            EvictionConfig evictionConfig = mapConfig.getEvictionConfig();
            maxSizePerNode = evictionConfig.getSize();

            assertEqualsStringFormat("Expected MaxSizePolicy %s, but was %s",
                    PER_NODE, evictionConfig.getMaxSizePolicy());
            assertTrue("Expected MaxSizePolicy.getSize() < Integer.MAX_VALUE",
                    maxSizePerNode < Integer.MAX_VALUE);

            logger.info("Eviction config of " + name + ": " + evictionConfig);
        }
    }

    @TimeStep(prob = 0.5)
    public void put(ThreadState state) {
        int key = state.randomInt(keyCount);
        map.put(key, state.randomInt());
        state.operationCounter.put++;
    }

    @TimeStep(prob = 0.2)
    public void putAsync(ThreadState state) {
        int key = state.randomInt(keyCount);
        map.putAsync(key, state.randomInt());
        state.operationCounter.putAsync++;
    }

    @TimeStep(prob = 0.2)
    public void get(ThreadState state) {
        int key = state.randomInt(keyCount);
        map.get(key);
        state.operationCounter.get++;
    }

    @TimeStep(prob = 0.1)
    public void check(ThreadState state) {
        assertMapMaxSize();
        state.operationCounter.verified++;
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        operationCounterList.add(state.operationCounter);
    }

    public class ThreadState extends BaseThreadState {
        private final MapMaxSizeOperationCounter operationCounter = new MapMaxSizeOperationCounter();
    }

    @Verify
    public void globalVerify() {
        MapMaxSizeOperationCounter total = new MapMaxSizeOperationCounter();
        for (MapMaxSizeOperationCounter operationCounter : operationCounterList) {
            total.add(operationCounter);
        }
        logger.info(format("Operation counters from %s: %s", name, total));

        assertMapMaxSize();
    }

    private void assertMapMaxSize() {
        if (isMemberNode(targetInstance)) {
            int mapSize = map.size();
            int clusterSize = targetInstance.getCluster().getMembers().size();
            assertTrue(format("Size of map %s should be <= %d * %d, but was %d", name, clusterSize, maxSizePerNode, mapSize),
                    mapSize <= clusterSize * maxSizePerNode);
        }
    }
}
