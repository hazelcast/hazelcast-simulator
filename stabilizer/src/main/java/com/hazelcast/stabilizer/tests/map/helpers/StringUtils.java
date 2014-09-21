package com.hazelcast.stabilizer.tests.map.helpers;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.stabilizer.tests.utils.KeyLocality;

import java.util.Random;

import static com.hazelcast.stabilizer.Utils.sleepSeconds;
import static com.hazelcast.stabilizer.tests.utils.TestUtils.isClient;

public class StringUtils {
    private final static String alphabet = "abcdefghijklmnopqrstuvwxyz1234567890";
    private final static Random random = new Random();

    public static String generateKey(int keyLength, KeyLocality keyLocality, HazelcastInstance instance) {
        switch (keyLocality) {
            case Local:
                return generateLocalKey(keyLength, instance);
            case Remote:
                return generateRemoteKey(keyLength, instance);
            case Random:
                return generateString(keyLength);
            default:
                throw new IllegalArgumentException("Unrecognized keyLocality:" + keyLocality);
        }
    }

    /**
     * Generates a key that is going to be stored on the remote instance. It can safely be called with a client instance, resulting
     * in a random key being returned.
     *
     * @param keyLength
     * @param instance
     * @return
     */
    public static String generateRemoteKey(int keyLength, HazelcastInstance instance) {
        if (isClient(instance)) {
            return generateString(keyLength);
        }

        for (; ; ) {
            String key = generateString(keyLength);
            if (!isLocalKey(instance, key)) {
                return key;
            }
        }
    }


    /**
     * Generates a key that is local to the given instance. It can safely be called with a client instance, resulting in
     * a random key being returned.
     *
     * @param keyLength
     * @param instance
     * @return
     */
    public static String generateLocalKey(int keyLength, HazelcastInstance instance) {
        if (isClient(instance)) {
            return generateString(keyLength);
        }

        for (; ; ) {
            String key = generateString(keyLength);
            if (isLocalKey(instance, key)) {
                return key;
            }
        }
    }

    private static boolean isLocalKey(HazelcastInstance instance, String key) {
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

    public static String generateString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < length; k++) {
            char c = alphabet.charAt(random.nextInt(alphabet.length()));
            sb.append(c);
        }

        return sb.toString();
    }
}
