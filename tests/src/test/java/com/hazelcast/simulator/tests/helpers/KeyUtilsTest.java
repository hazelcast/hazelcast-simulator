package com.hazelcast.simulator.tests.helpers;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.GroupProperties;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKey;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKey;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class KeyUtilsTest {

    private static final int PARTITION_COUNT = 10;

    @After
    public void teardown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(KeyUtils.class);
    }

    @Test
    public void generateInt_singlePartition() {
        int key1 = generateIntKey(100, KeyLocality.SINGLE_PARTITION, null);
        int key2 = generateIntKey(100, KeyLocality.SINGLE_PARTITION, null);

        assertEquals(0, key1);
        assertEquals(0, key2);
    }

    @Test
    public void generateString_singlePartition() {
        String key1 = generateStringKey(100, KeyLocality.SINGLE_PARTITION, null);
        String key2 = generateStringKey(100, KeyLocality.SINGLE_PARTITION, null);

        assertEquals("", key1);
        assertEquals("", key2);
    }

    @Test
    public void generateStringKeys_whenRandom_equalDistributionOverPartitions() throws InterruptedException {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, "" + PARTITION_COUNT);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        warmUpPartitions(hz);

        int keysPerPartition = 4;
        int keyCount = keysPerPartition * PARTITION_COUNT;
        String[] keys = KeyUtils.generateStringKeys(keyCount, "foo", KeyLocality.RANDOM, hz);

        assertEquals(keyCount, keys.length);

        int[] countPerPartition = new int[PARTITION_COUNT];
        for (String key : keys) {
            Partition partition = hz.getPartitionService().getPartition(key);
            countPerPartition[partition.getPartitionId()]++;
        }

        for (int count : countPerPartition) {
            assertEquals(keysPerPartition, count);
        }
    }

    @Test
    public void generateStringKeys_whenLocal_equalDistributionOverPartitions() throws InterruptedException {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, "" + PARTITION_COUNT);

        HazelcastInstance local = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance remote = Hazelcast.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        Map<Integer, Integer> countsPerPartition = new HashMap<Integer, Integer>();
        for (Partition partition : local.getPartitionService().getPartitions()) {
            if (partition.getOwner().localMember()) {
                countsPerPartition.put(partition.getPartitionId(), 0);
            }
        }

        int keysPerPartition = 4;
        int keyCount = countsPerPartition.size() * keysPerPartition;
        String[] keys = KeyUtils.generateStringKeys(keyCount, "foo", KeyLocality.LOCAL, local);

        assertEquals(keyCount, keys.length);

        for (String key : keys) {
            Partition partition = local.getPartitionService().getPartition(key);
            Integer count = countsPerPartition.get(partition.getPartitionId());
            assertNotNull(count);
            countsPerPartition.put(partition.getPartitionId(), count + 1);
        }

        System.out.println(countsPerPartition);
        for (int count : countsPerPartition.values()) {
            assertEquals(keysPerPartition, count);
        }
    }

    @Test
    public void generateStringKeys_whenRemote_equalDistributionOverPartitions() throws InterruptedException {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_PARTITION_COUNT, "" + PARTITION_COUNT);

        HazelcastInstance local = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance remote = Hazelcast.newHazelcastInstance(config);
        warmUpPartitions(local, remote);

        Map<Integer, Integer> countsPerPartition = new HashMap<Integer, Integer>();
        for (Partition partition : local.getPartitionService().getPartitions()) {
            if (!partition.getOwner().localMember()) {
                countsPerPartition.put(partition.getPartitionId(), 0);
            }
        }

        int keysPerPartition = 4;
        int keyCount = countsPerPartition.size() * keysPerPartition;
        String[] keys = KeyUtils.generateStringKeys(keyCount, "foo", KeyLocality.REMOTE, local);

        assertEquals(keyCount, keys.length);

        for (String key : keys) {
            Partition partition = local.getPartitionService().getPartition(key);
            Integer count = countsPerPartition.get(partition.getPartitionId());
            assertNotNull(count);
            countsPerPartition.put(partition.getPartitionId(), count + 1);
        }

        System.out.println(countsPerPartition);
        for (int count : countsPerPartition.values()) {
            assertEquals(keysPerPartition, count);
        }
    }

    private static void warmUpPartitions(HazelcastInstance... instances) throws InterruptedException {
        for (HazelcastInstance instance : instances) {
            final PartitionService ps = instance.getPartitionService();
            for (Partition partition : ps.getPartitions()) {
                while (partition.getOwner() == null) {
                    Thread.sleep(10);
                }
            }
        }
    }
}
