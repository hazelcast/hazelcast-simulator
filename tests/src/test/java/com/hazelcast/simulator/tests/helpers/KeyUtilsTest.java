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
package com.hazelcast.simulator.tests.helpers;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKey;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKey;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.isLocalKey;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class KeyUtilsTest {

    private static final int PARTITION_COUNT = 10;

    private static final Logger LOGGER = Logger.getLogger(KeyUtilsTest.class);

    private static HazelcastInstance instance;
    private static HazelcastInstance client;

    @BeforeClass
    public static void setUp() {
        Config config = new Config();
        config.setProperty("hazelcast.partition.count", "" + PARTITION_COUNT);

        instance = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance remoteInstance = Hazelcast.newHazelcastInstance(config);
        warmUpPartitions(instance, remoteInstance);

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

    @Test
    public void generateIntKey_local_client() {
        int key1 = generateIntKey(100, KeyLocality.LOCAL, client);
        int key2 = generateIntKey(100, KeyLocality.LOCAL, client);

        assertTrue(key1 >= 0 && key1 < 100);
        assertTrue(key2 >= 0 && key2 < 100);
    }

    @Test
    public void generateIntKey_remote_client() {
        int key1 = generateIntKey(100, KeyLocality.REMOTE, client);
        int key2 = generateIntKey(100, KeyLocality.REMOTE, client);

        assertTrue(key1 >= 0 && key1 < 100);
        assertTrue(key2 >= 0 && key2 < 100);
    }

    @Test
    public void generateIntKey_local() {
        int key1 = generateIntKey(100, KeyLocality.LOCAL, instance);
        int key2 = generateIntKey(100, KeyLocality.LOCAL, instance);

        assertTrue(key1 >= 0 && key1 < 100);
        assertTrue(key2 >= 0 && key2 < 100);
        assertTrue(isLocalKey(instance, key1));
        assertTrue(isLocalKey(instance, key2));
    }

    @Test
    public void generateStringKey_local() {
        String key1 = generateStringKey(100, KeyLocality.LOCAL, instance);
        String key2 = generateStringKey(100, KeyLocality.LOCAL, instance);

        assertNotNull(key1);
        assertNotNull(key2);
        assertTrue(!key1.isEmpty());
        assertTrue(!key2.isEmpty());
        assertNotEquals(key1, key2);
        assertTrue(isLocalKey(instance, key1));
        assertTrue(isLocalKey(instance, key2));
    }

    @Test
    public void generateIntKey_remote() {
        int key1 = generateIntKey(100, KeyLocality.REMOTE, instance);
        int key2 = generateIntKey(100, KeyLocality.REMOTE, instance);

        assertTrue(key1 >= 0 && key1 < 100);
        assertTrue(key2 >= 0 && key2 < 100);
        assertFalse(isLocalKey(instance, key1));
        assertFalse(isLocalKey(instance, key2));
    }

    @Test
    public void generateStringKey_remote() {
        String key1 = generateStringKey(100, KeyLocality.REMOTE, instance);
        String key2 = generateStringKey(100, KeyLocality.REMOTE, instance);

        assertNotNull(key1);
        assertNotNull(key2);
        assertTrue(!key1.isEmpty());
        assertTrue(!key2.isEmpty());
        assertNotEquals(key1, key2);
        assertFalse(isLocalKey(instance, key1));
        assertFalse(isLocalKey(instance, key2));
    }

    @Test
    public void generateIntKey_random() {
        int key1 = generateIntKey(100, KeyLocality.RANDOM, null);
        int key2 = generateIntKey(100, KeyLocality.RANDOM, null);

        assertTrue(key1 >= 0 && key1 < 100);
        assertTrue(key2 >= 0 && key2 < 100);
    }

    @Test
    public void generateStringKey_random() {
        String key1 = generateStringKey(100, KeyLocality.RANDOM, null);
        String key2 = generateStringKey(100, KeyLocality.RANDOM, null);

        assertNotNull(key1);
        assertNotNull(key2);
        assertTrue(!key1.isEmpty());
        assertTrue(!key2.isEmpty());
        assertNotEquals(key1, key2);
    }

    @Test
    public void generateIntKey_singlePartition() {
        int key1 = generateIntKey(100, KeyLocality.SINGLE_PARTITION, null);
        int key2 = generateIntKey(100, KeyLocality.SINGLE_PARTITION, null);

        assertEquals(0, key1);
        assertEquals(0, key2);
    }

    @Test
    public void generateStringKey_singlePartition() {
        String key1 = generateStringKey(100, KeyLocality.SINGLE_PARTITION, null);
        String key2 = generateStringKey(100, KeyLocality.SINGLE_PARTITION, null);

        assertEquals("", key1);
        assertEquals("", key2);
    }

    @Test
    public void generateIntKeys_singlePartition() {
        int[] keys = generateIntKeys(50, 100, KeyLocality.SINGLE_PARTITION, null);

        assertEquals(50, keys.length);
        for (int key : keys) {
            assertEquals(0, key);
        }
    }

    @Test
    public void generateStringKeys_singlePartition() {
        String[] keys = generateStringKeys(50, 100, KeyLocality.SINGLE_PARTITION, null);

        assertEquals(50, keys.length);
        for (String key : keys) {
            assertEquals("", key);
        }
    }

    @Test
    public void generateStringKeys_singlePartition_withPrefix() {
        String[] keys = generateStringKeys("test_", 10, 50, KeyLocality.SINGLE_PARTITION, instance);

        assertEquals(10, keys.length);
        for (String key : keys) {
            assertTrue(key.startsWith("test_"));
            assertEquals(50, key.length());
        }
    }

    @Test
    public void generateStringKeys_whenRandom_equalDistributionOverPartitions() {
        int keysPerPartition = 4;
        int keyCount = keysPerPartition * PARTITION_COUNT;
        String[] keys = KeyUtils.generateStringKeys("foo", keyCount, KeyLocality.RANDOM, instance);

        assertEquals(keyCount, keys.length);

        int[] countPerPartition = new int[PARTITION_COUNT];
        for (String key : keys) {
            Partition partition = instance.getPartitionService().getPartition(key);
            countPerPartition[partition.getPartitionId()]++;
        }

        for (int count : countPerPartition) {
            assertEquals(keysPerPartition, count);
        }
    }

    @Test
    public void generateStringKeys_whenLocal_equalDistributionOverPartitions() {
        Map<Integer, Integer> countsPerPartition = new HashMap<Integer, Integer>();
        for (Partition partition : instance.getPartitionService().getPartitions()) {
            if (partition.getOwner().localMember()) {
                countsPerPartition.put(partition.getPartitionId(), 0);
            }
        }

        int keysPerPartition = 4;
        int keyCount = countsPerPartition.size() * keysPerPartition;
        String[] keys = KeyUtils.generateStringKeys("foo", keyCount, KeyLocality.LOCAL, instance);

        assertEquals(keyCount, keys.length);

        for (String key : keys) {
            Partition partition = instance.getPartitionService().getPartition(key);
            Integer count = countsPerPartition.get(partition.getPartitionId());
            assertNotNull(count);
            countsPerPartition.put(partition.getPartitionId(), count + 1);
        }

        String localKey = keys[0];
        assertTrue(isLocalKey(instance, localKey));

        LOGGER.info(countsPerPartition);
        for (int count : countsPerPartition.values()) {
            assertEquals(keysPerPartition, count);
        }
    }

    @Test
    public void generateStringKeys_whenRemote_equalDistributionOverPartitions() {
        Map<Integer, Integer> countsPerPartition = new HashMap<Integer, Integer>();
        for (Partition partition : instance.getPartitionService().getPartitions()) {
            if (!partition.getOwner().localMember()) {
                countsPerPartition.put(partition.getPartitionId(), 0);
            }
        }

        int keysPerPartition = 4;
        int keyCount = countsPerPartition.size() * keysPerPartition;
        String[] keys = KeyUtils.generateStringKeys("foo", keyCount, KeyLocality.REMOTE, instance);

        assertEquals(keyCount, keys.length);

        for (String key : keys) {
            Partition partition = instance.getPartitionService().getPartition(key);
            Integer count = countsPerPartition.get(partition.getPartitionId());
            assertNotNull(count);
            countsPerPartition.put(partition.getPartitionId(), count + 1);
        }

        LOGGER.info(countsPerPartition);
        for (int count : countsPerPartition.values()) {
            assertEquals(keysPerPartition, count);
        }
    }

    private static void warmUpPartitions(HazelcastInstance... instances) {
        for (HazelcastInstance instance : instances) {
            PartitionService partitionService = instance.getPartitionService();
            for (Partition partition : partitionService.getPartitions()) {
                while (partition.getOwner() == null) {
                    sleepSeconds(1);
                }
            }
        }
    }
}
