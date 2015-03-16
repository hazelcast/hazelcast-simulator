package com.hazelcast.simulator.tests.helpers;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;

import java.util.Random;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isClient;

public class KeyUtils {

    public static int generateIntKey(int keyMaxValue, KeyLocality keyLocality, HazelcastInstance instance) {
        return generateKey(keyLocality, instance, new IntGenerator(keyMaxValue));
    }

    /**
     * Generates an array of key-strings with a configurable locality.
     *
     * If the instance is a client, keyLocality is ignored.
     *
     * @param keyCount the number of keys in the array.
     * @param keyMaxValue the size of of keys-strings
     * @param keyLocality if the key is local/remote/random
     * @param instance the HazelcastInstance that is used for keyLocality
     * @return the created array of keys
     */
    public static int[] generateIntKeys(int keyCount, int keyMaxValue, KeyLocality keyLocality, HazelcastInstance instance){
        int[] keys = new int[keyCount];
        for (int k = 0; k < keys.length; k++) {
            keys[k] = KeyUtils.generateIntKey(keyMaxValue, keyLocality, instance);
        }
        return keys;
    }

    public static String generateStringKey(int keyLength, KeyLocality keyLocality, HazelcastInstance instance) {
        return generateKey(keyLocality, instance, new StringGenerator(keyLength));
    }

    /**
     * Generates an array of key-strings with a configurable locality.
     *
     * If the instance is a client, keyLocality is ignored.
     *
     * @param keyCount the number of keys in the array.
     * @param keyLength the size of of keys-strings
     * @param keyLocality if the key is local/remote/random
     * @param instance the HazelcastInstance that is used for keyLocality
     * @return the created array of keys
     */
    public static String[] generateStringKeys(int keyCount, int keyLength, KeyLocality keyLocality, HazelcastInstance instance){
        String[] keys = new String[keyCount];
        for (int k = 0; k < keys.length; k++) {
            keys[k] = KeyUtils.generateStringKey(keyLength, keyLocality, instance);
        }
        return keys;
    }

    private static <T> T generateKey(KeyLocality keyLocality, HazelcastInstance instance, Generator<T> generator) {
        switch (keyLocality) {
            case Local:
                return generateLocalKey(generator, instance);
            case Remote:
                return generateRemoteKey(generator, instance);
            case Random:
                return generator.newKey();
            case SinglePartition:
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

    private interface Generator<K> {
        K newKey();

        K newConstantKey();
    }

    private static class StringGenerator implements Generator<String> {
        private int length;
        private StringGenerator(int length) {
            this.length = length;
        }

        @Override
        public String newKey() {
            return StringUtils.generateString(length);
        }

        @Override
        public String newConstantKey() {
            return "";
        }
    }

    private static class IntGenerator implements Generator<Integer> {
        private static Random random = new Random();
        private int n;

        private IntGenerator(int n) {
            this.n = n;
        }

        @Override
        public Integer newKey() {
            return random.nextInt(n);
        }

        @Override
        public Integer newConstantKey() {
            return 0;
        }
    }
}
