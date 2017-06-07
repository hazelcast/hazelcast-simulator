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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getNode;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;

public class BackupExpirationMapTest extends AbstractTest {

    // properties
    public int minNumberOfMembers = 0;
    public int keyCount = 10000;
    public KeyLocality keyLocality = KeyLocality.SHARED;

    private IMap<Integer, Integer> map;
    private int[] keys;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
    }

    @Prepare(global = false)
    public void prepare() {
        waitClusterSize(logger, targetInstance, minNumberOfMembers);
        keys = generateIntKeys(keyCount, keyLocality, targetInstance);
        Streamer<Integer, Integer> streamer = StreamerFactory.getInstance(map);
        Random random = new Random();
        for (int key : keys) {
            int value = random.nextInt(Integer.MAX_VALUE);
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @TimeStep(prob = 0.1)
    public Integer put(ThreadState state) {
        int key = state.randomKey();
        int value = state.randomValue();
        return map.put(key, value);
    }

    @TimeStep(prob = -1)
    public Integer get(ThreadState state) {
        int key = state.randomKey();
        return map.get(key);
    }

    @Verify(global = false)
    public void verify() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                int countOnNode = entryCountOnNode(name, targetInstance);
                assertEquals("countOnNode=" + countOnNode, 0, countOnNode);
            }
        }, 600);
    }

    /**
     * @return number of entries in this maps recordstores on supplied node(regardless of owner or backup)
     */
    private static int entryCountOnNode(String mapName, HazelcastInstance hazelcastInstance) {
        Node node = getNode(hazelcastInstance);
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();

        int size = 0;
        for (int partitionID = 0; partitionID < partitionCount; partitionID++) {
            RecordStore recordStore = mapServiceContext.getExistingRecordStore(partitionID, mapName);
            if (recordStore != null) {
                size += recordStore.size();
            }
        }
        return size;
    }

    public class ThreadState extends BaseThreadState {

        private int randomKey() {
            return keys[randomInt(keys.length)];
        }

        private int randomValue() {
            return randomInt(Integer.MAX_VALUE);
        }
    }
}
