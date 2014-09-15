package com.hazelcast.stabilizer.tests.map.helpers;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;

import java.util.Random;

import static com.hazelcast.stabilizer.Utils.sleepSeconds;

public class StringUtils {
    private final static String alphabet = "abcdefghijklmnopqrstuvwxyz1234567890";
    private final static Random random = new Random();

    public static String generateKey(int keyLength, boolean preventLocalCalls, HazelcastInstance instance) {
        if (!preventLocalCalls) {
            return makeString(keyLength);
        }
        for (; ; ) {
            String key = makeString(keyLength);
            if (!isLocalKey(instance, key)) {
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

    public static String makeString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < length; k++) {
            char c = alphabet.charAt(random.nextInt(alphabet.length()));
            sb.append(c);
        }

        return sb.toString();
    }
}
