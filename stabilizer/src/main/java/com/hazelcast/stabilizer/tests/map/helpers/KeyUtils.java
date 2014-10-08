package com.hazelcast.stabilizer.tests.map.helpers;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.stabilizer.tests.utils.KeyLocality;

import java.util.Random;

import static com.hazelcast.stabilizer.Utils.sleepSeconds;
import static com.hazelcast.stabilizer.tests.utils.TestUtils.isClient;

public class KeyUtils {

    public static int generateInt(int max, KeyLocality keyLocality, HazelcastInstance instance) {
        return generateKey(keyLocality, instance, new IntGenerator(max));
    }

    public static String generateStringKey(int keyLength, KeyLocality keyLocality, HazelcastInstance instance) {
        return generateKey(keyLocality, instance, new StringGenerator(keyLength));
    }

    public static <T> T generateKey(KeyLocality keyLocality, HazelcastInstance instance, Generator<T> generator) {
        switch (keyLocality) {
            case Local:
                return generateLocalKey(generator, instance);
            case Remote:
                return generateRemoteKey(generator, instance);
            case Random:
                return generator.newKey();
            default:
                throw new IllegalArgumentException("Unrecognized keyLocality:" + keyLocality);
        }
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
    public static String[] generateKeys(int keyCount, int keyLength, KeyLocality keyLocality, HazelcastInstance instance){
        String[] keys = new String[keyCount];
        for (int k = 0; k < keys.length; k++) {
            keys[k] = KeyUtils.generateStringKey(keyLength, keyLocality, instance);
        }
        return keys;
    }


    /**
     * Generates a key that is going to be stored on the remote instance. It can safely be called with a client instance, resulting
     * in a random key being returned.
     *
     * @param generator
     * @param instance
     * @return
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


    /**
     * Generates a key that is local to the given instance. It can safely be called with a client instance, resulting in
     * a random key being returned.
     *
     * @param generator
     * @param instance
     * @return
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
    }
}
