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
import com.hazelcast.partition.Partition;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.core.Hazelcast.newHazelcastInstance;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntegerKeys;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKey;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.isLocalKey;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.hazelcast5.Hazelcast5Driver.warmupPartitions;
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
    public static void beforeClass() {
        Config config = new Config();
        config.setProperty("hazelcast.partition.count", "" + PARTITION_COUNT);

        hz = newHazelcastInstance(config);
        HazelcastInstance remoteInstance = newHazelcastInstance(config);
        warmupPartitions(hz);
        warmupPartitions(remoteInstance);

        ClientConfig clientconfig = new ClientConfig();
        clientconfig.setProperty("hazelcast.partition.count", "" + PARTITION_COUNT);

        client = HazelcastClient.newHazelcastClient(clientconfig);
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(KeyUtils.class);
    }

    // =========================== generateIntKeys() =============================

    @Test
    public void testGenerateIntKeys_whenLocal_equalDistributionOverPartitions() {
        Map<Integer, Integer> countsPerPartition = new HashMap<Integer, Integer>();
        for (Partition partition : hz.getPartitionService().getPartitions()) {
            if (partition.getOwner().localMember()) {
                countsPerPartition.put(partition.getPartitionId(), 0);
            }
        }

        int keysPerPartition = 4;
        int keyCount = countsPerPartition.size() * keysPerPartition;
        int[] keys = generateIntKeys(keyCount, KeyLocality.LOCAL, hz);

        assertEquals(keyCount, keys.length);

        for (int key : keys) {
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
    public void testGenerateIntKeys_whenRemote_equalDistributionOverPartitions() {
        Map<Integer, Integer> countsPerPartition = new HashMap<Integer, Integer>();
        for (Partition partition : hz.getPartitionService().getPartitions()) {
            if (!partition.getOwner().localMember()) {
                countsPerPartition.put(partition.getPartitionId(), 0);
            }
        }

        int keysPerPartition = 4;
        int keyCount = countsPerPartition.size() * keysPerPartition;
        int[] keys = generateIntKeys(keyCount, KeyLocality.REMOTE, hz);

        assertEquals(keyCount, keys.length);

        for (int key : keys) {
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
    public void testGenerateIntKeys_whenRandom_equalDistributionOverPartitions() {
        int keysPerPartition = 4;
        int keyCount = keysPerPartition * PARTITION_COUNT;
        int[] keys = generateIntKeys(keyCount, KeyLocality.RANDOM, hz);

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
    public void testGenerateIntKeys_whenLocal_client() {
        int[] keys = generateIntKeys(2, KeyLocality.LOCAL, client);

        assertEquals(2, keys.length);
    }

    @Test
    public void testGenerateIntKeys_whenShared() {
        int[] keys = generateIntKeys(100, KeyLocality.SHARED, null);

        assertEquals(100, keys.length);
        for (int i = 0; i < keys.length; i++) {
            assertEquals(i, keys[i]);
        }
    }

    @Test
    public void testGenerateIntKeys_whenSinglePartition() {
        int[] keys = generateIntKeys(10, KeyLocality.SINGLE_PARTITION, null);

        assertEquals(10, keys.length);
        for (int key : keys) {
            assertEquals(0, key);
        }
    }

    // =========================== generateIntegerKeys() =============================

    @Test
    public void testGenerateIntegerKeys_whenLocal_equalDistributionOverPartitions() {
        Map<Integer, Integer> countsPerPartition = new HashMap<Integer, Integer>();
        for (Partition partition : hz.getPartitionService().getPartitions()) {
            if (partition.getOwner().localMember()) {
                countsPerPartition.put(partition.getPartitionId(), 0);
            }
        }

        int keysPerPartition = 4;
        int keyCount = countsPerPartition.size() * keysPerPartition;
        Integer[] keys = generateIntegerKeys(keyCount, KeyLocality.LOCAL, hz);

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
    public void testGenerateIntegerKeys_whenRemote_equalDistributionOverPartitions() {
        Map<Integer, Integer> countsPerPartition = new HashMap<Integer, Integer>();
        for (Partition partition : hz.getPartitionService().getPartitions()) {
            if (!partition.getOwner().localMember()) {
                countsPerPartition.put(partition.getPartitionId(), 0);
            }
        }

        int keysPerPartition = 4;
        int keyCount = countsPerPartition.size() * keysPerPartition;
        Integer[] keys = generateIntegerKeys(keyCount, KeyLocality.REMOTE, hz);

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
    public void testGenerateIntegerKeys_whenRandom_equalDistributionOverPartitions() {
        int keysPerPartition = 4;
        int keyCount = keysPerPartition * PARTITION_COUNT;
        Integer[] keys = generateIntegerKeys(keyCount, KeyLocality.RANDOM, hz);

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
    public void testGenerateIntegerKeys_whenLocal_client() {
        Integer[] keys = generateIntegerKeys(2, KeyLocality.LOCAL, client);

        assertEquals(2, keys.length);
    }

    @Test
    public void testGenerateIntegerKeys_whenShared() {
        Integer[] keys = generateIntegerKeys(100, KeyLocality.SHARED, null);

        assertEquals(100, keys.length);
        for (int i = 0; i < keys.length; i++) {
            assertEquals(i, keys[i].intValue());
        }
    }

    @Test
    public void testGenerateIntegerKeys_whenSinglePartition() {
        Integer[] keys = generateIntegerKeys(10, KeyLocality.SINGLE_PARTITION, null);

        assertEquals(10, keys.length);
        for (int key : keys) {
            assertEquals(0, key);
        }
    }

    // =========================== generateStringKey() =============================

    @Test
    public void testGenerateStringKey_whenLocal() {
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
    public void testGenerateStringKey_whenRemote() {
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
    public void testGenerateStringKey_whenRandom() {
        String key1 = generateStringKey(100, KeyLocality.RANDOM, hz);
        String key2 = generateStringKey(100, KeyLocality.RANDOM, hz);

        assertNotNull(key1);
        assertNotNull(key2);
        assertTrue(!key1.isEmpty());
        assertTrue(!key2.isEmpty());
        assertNotEquals(key1, key2);
    }

    @Test
    public void testGenerateStringKey_whenShared() {
        String key = generateStringKey(10, KeyLocality.SHARED, null);

        assertEquals("0000000000", key);
    }

    @Test
    public void testGenerateStringKey_whenSinglePartition() {
        String key = generateStringKey(10, KeyLocality.SINGLE_PARTITION, null);

        assertEquals("0000000000", key);
    }

    // =========================== generateStringKeys() =============================

    @Test
    public void testGenerateStringKeys_whenLocal_equalDistributionOverPartitions() {
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
    public void testGenerateStringKeys_whenRemote_equalDistributionOverPartitions() {
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

    @Test
    public void testGenerateStringKeys_whenRandom_equalDistributionOverPartitions() {
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
    public void testGenerateStringKeys_whenShared() {
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
    public void testGenerateStringKeys_whenSinglePartition() {
        String[] keys = generateStringKeys(50, 10, KeyLocality.SINGLE_PARTITION, null);

        assertEquals(50, keys.length);
        for (String key : keys) {
            assertEquals("0000000000", key);
        }
    }

    @Test
    public void testGenerateStringKeys_whenSinglePartition_withPrefix() {
        String[] keys = generateStringKeys("prefix", 50, 10, KeyLocality.SINGLE_PARTITION, null);

        assertEquals(50, keys.length);
        for (String key : keys) {
            assertEquals("prefix0000", key);
        }
    }

    // =========================== BalancedKeyGenerator =============================

    @Test(expected = IllegalArgumentException.class)
    public void testBalancedKeyGenerator_withUnsupportedKeyLocality() {
        new KeyUtils.BalancedKeyGenerator<Integer>(hz, KeyLocality.SHARED, 0) {

            @Override
            protected Integer generateKey() {
                return null;
            }
        };
    }

    @Test
    public void testBalancedKeyGenerator_willGenerateNewKeyOnDuplicateKey() {
        GenerateSameKeyOnceGenerator generator = new GenerateSameKeyOnceGenerator(hz);

        assertEquals(0, generator.next().intValue());
        assertEquals(1, generator.next().intValue());
    }

    private static class GenerateSameKeyOnceGenerator extends KeyUtils.BalancedKeyGenerator<Integer> {

        private static final Integer[] KEYS = new Integer[]{new Integer(0), new Integer(0), new Integer(1)};

        private int keyIndex;

        private GenerateSameKeyOnceGenerator(HazelcastInstance hz) {
            super(hz, KeyLocality.RANDOM, 2);
        }

        @Override
        protected Integer generateKey() {
            return KEYS[keyIndex++];
        }
    }
}
