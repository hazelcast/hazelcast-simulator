package com.hazelcast.simulator.tests.helpers;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;

import java.util.Random;

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
     * @param instance    the HazelcastInstance the key should belong to
     * @param key         the key to check
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
