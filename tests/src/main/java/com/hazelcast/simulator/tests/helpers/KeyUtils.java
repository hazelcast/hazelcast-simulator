package com.hazelcast.simulator.tests.helpers;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isClient;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateString;

public final class KeyUtils {

    private KeyUtils() {
    }

    /**
     * Generates an int key with a configurable locality.
     *
     * @param keyMaxValue max value of the key
     * @param keyLocality if the key is local/remote/random
     * @param instance    the HazelcastInstance that is used for keyLocality
     * @return the created key
     */
    public static int generateIntKey(int keyMaxValue, KeyLocality keyLocality, HazelcastInstance instance) {
        return generateKey(keyLocality, instance, new IntGenerator(keyMaxValue));
    }

    /**
     * Generates an array of int keys with a configurable locality.
     *
     * If the instance is a client, keyLocality is ignored.
     *
     * @param keyCount    the number of keys in the array
     * @param keyMaxValue max value of the key
     * @param keyLocality if the key is local/remote/random
     * @param instance    the HazelcastInstance that is used for keyLocality
     * @return the created array of keys
     */
    public static int[] generateIntKeys(int keyCount, int keyMaxValue, KeyLocality keyLocality, HazelcastInstance instance) {
        int[] keys = new int[keyCount];
        for (int k = 0; k < keys.length; k++) {
            keys[k] = generateIntKey(keyMaxValue, keyLocality, instance);
        }
        return keys;
    }

    /**
     * Generates a string key with a configurable locality.
     *
     * @param keyLength   the length of each string key
     * @param keyLocality if the key is local/remote/random
     * @param instance    the HazelcastInstance that is used for keyLocality
     * @return the created key
     */
    public static String generateStringKey(int keyLength, KeyLocality keyLocality, HazelcastInstance instance) {
        return generateKey(keyLocality, instance, new StringGenerator(keyLength));
    }

    public static String[] generateStringKeys(int keyCount, String basename, KeyLocality keyLocality, HazelcastInstance instance) {
        int keyLength = (int) (basename.length() + Math.ceil(Math.log10(keyCount)));
        return generateStringKeys(keyLength, keyCount, basename, keyLocality, instance);
    }

    public static String[] generateStringKeys(int keyLength, int keyCount, String basename, KeyLocality keyLocality, HazelcastInstance instance) {
        Set<Integer> targetPartitions = getTargetPartitions(keyLocality, instance);
        PartitionService partitionService = instance.getPartitionService();

        Map<Integer, Set<String>> keysPerPartitionMap = new HashMap<Integer, Set<String>>();
        for (Integer partitionId : targetPartitions) {
            keysPerPartitionMap.put(partitionId, new HashSet<String>());
        }

        int maxKeysPerPartition = (int) Math.ceil(keyCount / targetPartitions.size());

        int generatedKeyCount = 0;
        for (; ; ) {
            String key = basename + generateString(keyLength - basename.length());
            Partition partition = partitionService.getPartition(key);
            Set<String> keysPerPartition = keysPerPartitionMap.get(partition.getPartitionId());

            if (keysPerPartition == null) {
                // we are not interested in this key.
                continue;
            }

            if (keysPerPartition.size() == maxKeysPerPartition) {
                // we have reached the maximum number of keys for this given partition
                continue;
            }

            if (!keysPerPartition.add(key)) {
                // duplicate key, we can ignore it
                continue;
            }

            generatedKeyCount++;
            if (generatedKeyCount == keyCount) {
                break;
            }
        }

        String[] result = new String[keyCount];
        int index = 0;
        for (Set<String> keysPerPartition : keysPerPartitionMap.values()) {
            for (String s : keysPerPartition) {
                result[index] = s;
                index++;
            }
        }
        return result;
    }

    private static Set<Integer> getTargetPartitions(KeyLocality keyLocality, HazelcastInstance hz) {
        Set<Integer> targetPartitions = new HashSet<Integer>();
        PartitionService partitionService = hz.getPartitionService();
        Member localMember = hz.getCluster().getLocalMember();
        switch (keyLocality) {
            case LOCAL:
                for (Partition partition : partitionService.getPartitions()) {
                    if (localMember == null || localMember.equals(partition.getOwner())) {
                        targetPartitions.add(partition.getPartitionId());
                    }
                }
                break;
            case RANDOM:
                for (Partition partition : partitionService.getPartitions()) {
                    targetPartitions.add(partition.getPartitionId());
                }
                break;
            case REMOTE:
                for (Partition partition : partitionService.getPartitions()) {
                    if (localMember == null || !localMember.equals(partition.getOwner())) {
                        targetPartitions.add(partition.getPartitionId());
                    }
                }
                break;
            case SINGLE_PARTITION:
                targetPartitions.add(0);
                break;
        }
        return targetPartitions;
    }


    /**
     * Generates an array of string keys with a configurable locality.
     *
     * If the instance is a client, keyLocality is ignored.
     *
     * @param keyCount    the number of keys in the array
     * @param keyLength   the length of each string key
     * @param keyLocality if the key is local/remote/random
     * @param instance    the HazelcastInstance that is used for keyLocality
     * @return the created array of keys
     */
    public static String[] generateStringKeys(int keyCount, int keyLength, KeyLocality keyLocality, HazelcastInstance instance) {
        String[] keys = new String[keyCount];
        for (int k = 0; k < keys.length; k++) {
            keys[k] = generateStringKey(keyLength, keyLocality, instance);
        }
        return keys;
    }

    /**
     * Checks if a key is located on a Hazelcast instance.
     *
     * @param instance the HazelcastInstance the key should belong to
     * @param key      the key to check
     * @return <tt>true</tt> if the key belongs to the Hazelcast instance, <tt>false</tt> otherwise
     */
    public static boolean isLocalKey(HazelcastInstance instance, Object key) {
        PartitionService partitionService = instance.getPartitionService();
        Partition partition = partitionService.getPartition(key);
        for (; ; ) {
            Member owner = partition.getOwner();
            if (owner == null) {
                sleepSeconds(1);
                continue;
            }
            return owner.equals(instance.getLocalEndpoint());
        }
    }

    private static <T> T generateKey(KeyLocality keyLocality, HazelcastInstance instance, Generator<T> generator) {
        switch (keyLocality) {
            case LOCAL:
                return generateLocalKey(generator, instance);
            case REMOTE:
                return generateRemoteKey(generator, instance);
            case RANDOM:
                return generator.newKey();
            case SINGLE_PARTITION:
                return generator.newConstantKey();
            default:
                throw new IllegalArgumentException("Unrecognized keyLocality:" + keyLocality);
        }
    }

    /**
     * Generates a key that is local to the given instance. It can safely be called with a client instance, resulting in
     * a random key being returned.
     */
    private static <T> T generateLocalKey(Generator<T> generator, HazelcastInstance instance) {
        if (isClient(instance)) {
            return generator.newKey();
        }

        for (; ; ) {
            T key = generator.newKey();
            if (isLocalKey(instance, key)) {
                return key;
            }
        }
    }

    /**
     * Generates a key that is going to be stored on the remote instance. It can safely be called with a client
     * instance, resulting in a random key being returned.
     */
    private static <T> T generateRemoteKey(Generator<T> generator, HazelcastInstance instance) {
        if (isClient(instance)) {
            return generator.newKey();
        }

        for (; ; ) {
            T key = generator.newKey();
            if (!isLocalKey(instance, key)) {
                return key;
            }
        }
    }

    private interface Generator<K> {

        K newKey();

        K newConstantKey();
    }

    private static final class StringGenerator implements Generator<String> {

        private final int length;

        private StringGenerator(int length) {
            this.length = length;
        }

        @Override
        public String newKey() {
            return generateString(length);
        }

        @Override
        public String newConstantKey() {
            return "";
        }
    }

    private static final class IntGenerator implements Generator<Integer> {

        private static final Random RANDOM = new Random();

        private final int maxValue;

        private IntGenerator(int maxValue) {
            this.maxValue = maxValue;
        }

        @Override
        public Integer newKey() {
            return RANDOM.nextInt(maxValue);
        }

        @Override
        public Integer newConstantKey() {
            return 0;
        }
    }
}
