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
package com.hazelcast.simulator.tests.helpers;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKey;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.isLocalKey;
import static com.hazelcast.simulator.utils.HazelcastUtils.warmupPartitions;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class KeyUtilsTest {

    private static final int PARTITION_COUNT = 10;

    private static final Logger LOGGER = Logger.getLogger(KeyUtilsTest.class);

    private static HazelcastInstance hz;
    private static HazelcastInstance client;

    @BeforeClass
    public static void setUp() {
        Config config = new Config();
        config.setProperty("hazelcast.partition.count", "" + PARTITION_COUNT);

        hz = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance remoteInstance = Hazelcast.newHazelcastInstance(config);
        warmupPartitions(hz);
        warmupPartitions(remoteInstance);

        ClientConfig clientconfig = new ClientConfig();
        clientconfig.setProperty("hazelcast.partition.count", "" + PARTITION_COUNT);

        client = HazelcastClient.newHazelcastClient(clientconfig);
    }

    @AfterClass
    public static void tearDown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(KeyUtils.class);
    }

    // =========================== generate int keys =============================

    @Test
    public void generateIntKeys_whenLocal_equalDistributionOverPartitions() {
        Map<Integer, Integer> countsPerPartition = new HashMap<Integer, Integer>();
        for (Partition partition : hz.getPartitionService().getPartitions()) {
            if (partition.getOwner().localMember()) {
                countsPerPartition.put(partition.getPartitionId(), 0);
            }
        }

        int keysPerPartition = 4;
        int keyCount = countsPerPartition.size() * keysPerPartition;
        int[] keys = KeyUtils.generateIntKeys(keyCount, KeyLocality.LOCAL, hz);

        assertEquals(keyCount, keys.length);

        for (Integer key : keys) {
            assertTrue(isLocalKey(hz, key));

            Partition partition = hz.getPartitionService().getPartition(key);
            Integer count = countsPerPartition.get(partition.getPartitionId());
            assertNotNull(count);
            countsPerPartition.put(partition.getPartitionId(), count + 1);
        }

        LOGGER.info(countsPerPartition);
        for (int count : countsPerPartition.values()) {
            assertEquals(keysPerPartition, count);
        }
    }

    @Test
    public void generateIntKeys_whenRemote_equalDistributionOverPartitions() {
        Map<Integer, Integer> countsPerPartition = new HashMap<Integer, Integer>();
        for (Partition partition : hz.getPartitionService().getPartitions()) {
            if (!partition.getOwner().localMember()) {
                countsPerPartition.put(partition.getPartitionId(), 0);
            }
        }

        int keysPerPartition = 4;
        int keyCount = countsPerPartition.size() * keysPerPartition;
        int[] keys = KeyUtils.generateIntKeys(keyCount, KeyLocality.REMOTE, hz);

        assertEquals(keyCount, keys.length);

        for (Integer key : keys) {
            assertFalse(isLocalKey(hz, key));

            Partition partition = hz.getPartitionService().getPartition(key);
            Integer count = countsPerPartition.get(partition.getPartitionId());
            assertNotNull(count);
            countsPerPartition.put(partition.getPartitionId(), count + 1);
        }

        LOGGER.info(countsPerPartition);
        for (int count : countsPerPartition.values()) {
            assertEquals(keysPerPartition, count);
        }
    }

    @Test
    public void generateIntKeys_whenRandom_equalDistributionOverPartitions() {
        int keysPerPartition = 4;
        int keyCount = keysPerPartition * PARTITION_COUNT;
        int[] keys = KeyUtils.generateIntKeys(keyCount, KeyLocality.RANDOM, hz);

        assertEquals(keyCount, keys.length);

        int[] countPerPartition = new int[PARTITION_COUNT];
        for (Integer key : keys) {
            Partition partition = hz.getPartitionService().getPartition(key);
            countPerPartition[partition.getPartitionId()]++;
        }

        for (int count : countPerPartition) {
            assertEquals(keysPerPartition, count);
        }
    }

    @Test
    public void generateIntKey_local_client() {
        int[] keys = generateIntKeys(2, KeyLocality.LOCAL, client);

        assertEquals(2, keys.length);
    }

    @Test
    public void generateIntKeys_shared() {
        int[] keys = generateIntKeys(100, KeyLocality.SHARED, null);

        assertEquals(100, keys.length);
        for (int i = 0; i < keys.length; i++) {
            assertEquals(i, keys[i]);
        }
    }

    @Test
    public void generateIntKeys_singlePartition() {
        int[] keys = generateIntKeys(10, KeyLocality.SINGLE_PARTITION, null);

        assertEquals(10, keys.length);
        for (int key : keys) {
            assertEquals(0, key);
        }
    }

    // =========================== generate String key =============================

    @Test
    public void generateStringKey_singlePartition() {
        String key = generateStringKey(10, KeyLocality.SINGLE_PARTITION, null);

        assertEquals("0000000000", key);
    }

    @Test
    public void generateStringKey_shared() {
        String key = generateStringKey(10, KeyLocality.SHARED, null);

        assertEquals("0000000000", key);
    }

    @Test
    public void generateStringKey_local() {
        String key1 = generateStringKey(100, KeyLocality.LOCAL, hz);
        String key2 = generateStringKey(100, KeyLocality.LOCAL, hz);

        assertNotNull(key1);
        assertNotNull(key2);
        assertTrue(!key1.isEmpty());
        assertTrue(!key2.isEmpty());
        assertNotEquals(key1, key2);
        assertTrue(isLocalKey(hz, key1));
        assertTrue(isLocalKey(hz, key2));
    }

    @Test
    public void generateStringKey_remote() {
        String key1 = generateStringKey(100, KeyLocality.REMOTE, hz);
        String key2 = generateStringKey(100, KeyLocality.REMOTE, hz);

        assertNotNull(key1);
        assertNotNull(key2);
        assertTrue(!key1.isEmpty());
        assertTrue(!key2.isEmpty());
        assertNotEquals(key1, key2);
        assertFalse(isLocalKey(hz, key1));
        assertFalse(isLocalKey(hz, key2));
    }

    @Test
    public void generateStringKey_random() {
        String key1 = generateStringKey(100, KeyLocality.RANDOM, hz);
        String key2 = generateStringKey(100, KeyLocality.RANDOM, hz);

        assertNotNull(key1);
        assertNotNull(key2);
        assertTrue(!key1.isEmpty());
        assertTrue(!key2.isEmpty());
        assertNotEquals(key1, key2);
    }

    // =========================== generate String keys =============================

    @Test
    public void generateStringKeys_singlePartition() {
        String[] keys = generateStringKeys(50, 10, KeyLocality.SINGLE_PARTITION, null);

        assertEquals(50, keys.length);
        for (String key : keys) {
            assertEquals("0000000000", key);
        }
    }

    @Test
    public void generateStringKeys_singlePartition_prefix() {
        String[] keys = generateStringKeys("prefix", 50, 10, KeyLocality.SINGLE_PARTITION, null);

        assertEquals(50, keys.length);
        for (String key : keys) {
            assertEquals("prefix0000", key);
        }
    }

    @Test
    public void generateStringKeys_shared() {
        String[] keys = generateStringKeys("prefix", 12, 10, KeyLocality.SHARED, null);

        assertEquals(12, keys.length);
        assertEquals(keys[0], "prefix0000");
        assertEquals(keys[1], "prefix0001");
        assertEquals(keys[2], "prefix0002");
        assertEquals(keys[3], "prefix0003");
        assertEquals(keys[4], "prefix0004");
        assertEquals(keys[5], "prefix0005");
        assertEquals(keys[6], "prefix0006");
        assertEquals(keys[7], "prefix0007");
        assertEquals(keys[8], "prefix0008");
        assertEquals(keys[9], "prefix0009");
        assertEquals(keys[10], "prefix0010");
        assertEquals(keys[11], "prefix0011");
    }

    @Test
    public void generateStringKeys_whenRandom_equalDistributionOverPartitions() {
        int keysPerPartition = 4;
        int keyCount = keysPerPartition * PARTITION_COUNT;
        String[] keys = generateStringKeys("prefix", keyCount, KeyLocality.RANDOM, hz);

        assertEquals(keyCount, keys.length);

        int[] countPerPartition = new int[PARTITION_COUNT];
        for (String key : keys) {
            Partition partition = hz.getPartitionService().getPartition(key);
            countPerPartition[partition.getPartitionId()]++;
            assertTrue(key.startsWith("prefix"));
        }

        for (int count : countPerPartition) {
            assertEquals(keysPerPartition, count);
        }
    }

    @Test
    public void generateStringKeys_whenLocal_equalDistributionOverPartitions() {
        Map<Integer, Integer> countsPerPartition = new HashMap<Integer, Integer>();
        for (Partition partition : hz.getPartitionService().getPartitions()) {
            if (partition.getOwner().localMember()) {
                countsPerPartition.put(partition.getPartitionId(), 0);
            }
        }

        int keysPerPartition = 4;
        int keyCount = countsPerPartition.size() * keysPerPartition;
        String[] keys = generateStringKeys("prefix", keyCount, KeyLocality.LOCAL, hz);

        assertEquals(keyCount, keys.length);

        for (String key : keys) {
            assertTrue(key.startsWith("prefix"));
            assertTrue(isLocalKey(hz, key));

            Partition partition = hz.getPartitionService().getPartition(key);
            Integer count = countsPerPartition.get(partition.getPartitionId());
            assertNotNull(count);
            countsPerPartition.put(partition.getPartitionId(), count + 1);
        }

        String localKey = keys[0];
        assertTrue(isLocalKey(hz, localKey));

        LOGGER.info(countsPerPartition);
        for (int count : countsPerPartition.values()) {
            assertEquals(keysPerPartition, count);
        }
    }

    @Test
    public void generateStringKeys_whenRemote_equalDistributionOverPartitions() {
        Map<Integer, Integer> countsPerPartition = new HashMap<Integer, Integer>();
        for (Partition partition : hz.getPartitionService().getPartitions()) {
            if (!partition.getOwner().localMember()) {
                countsPerPartition.put(partition.getPartitionId(), 0);
            }
        }

        int keysPerPartition = 4;
        int keyCount = countsPerPartition.size() * keysPerPartition;
        String[] keys = generateStringKeys("prefix", keyCount, KeyLocality.REMOTE, hz);

        assertEquals(keyCount, keys.length);

        for (String key : keys) {
            assertTrue(key.startsWith("prefix"));
            assertFalse(isLocalKey(hz, key));

            Partition partition = hz.getPartitionService().getPartition(key);
            Integer count = countsPerPartition.get(partition.getPartitionId());
            assertNotNull(count);
            countsPerPartition.put(partition.getPartitionId(), count + 1);
        }

        LOGGER.info(countsPerPartition);
        for (int count : countsPerPartition.values()) {
            assertEquals(keysPerPartition, count);
        }
    }
}
