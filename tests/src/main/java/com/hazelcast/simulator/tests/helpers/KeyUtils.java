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
     * Checks if a key is located on a Hazelcast instance.
     *
     * @param instance the HazelcastInstance the key should belong to
     * @param key      the key to check
     * @return <tt>true</tt> if the key belongs to the Hazelcast instance, <tt>false</tt> otherwise
     */
    public static boolean isLocalKey(HazelcastInstance instance, Object key) {
        PartitionService partitionService = instance.getPartitionService();
        Partition partition = partitionService.getPartition(key);
        Member owner;
        while (true) {
            owner = partition.getOwner();
            if (owner != null) {
                break;
            }
            sleepSeconds(1);
        }
        return owner.equals(instance.getLocalEndpoint());
    }

    /**
     * Generates an int key with a configurable keyLocality.
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
     * Generates an array of int keys with a configurable keyLocality.
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
        for (int i = 0; i < keys.length; i++) {
            keys[i] = generateIntKey(keyMaxValue, keyLocality, instance);
        }
        return keys;
    }

    /**
     * Generates a string key with a configurable keyLocality.
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
     * Generates an array of string keys with a configurable keyLocality.
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
        for (int i = 0; i < keys.length; i++) {
            keys[i] = generateStringKey(keyLength, keyLocality, instance);
        }
        return keys;
    }

    /**
     * Generates an array of string keys with a configurable keyLocality.
     *
     * If the instance is a client, keyLocality is ignored.
     *
     * @param prefix      prefix for the generated keys
     * @param keyCount    the number of keys in the array
     * @param keyLocality if the key is local/remote/random
     * @param instance    the HazelcastInstance that is used for keyLocality
     * @return the created array of keys
     */
    public static String[] generateStringKeys(String prefix, int keyCount, KeyLocality keyLocality, HazelcastInstance instance) {
        int keyLength = (int) (prefix.length() + Math.ceil(Math.log10(keyCount))) + 2;
        return generateStringKeys(prefix, keyCount, keyLength, keyLocality, instance);
    }

    /**
     * Generates an array of string keys with a configurable keyLocality.
     *
     * If the instance is a client, keyLocality is ignored.
     *
     * @param prefix      prefix for the generated keys
     * @param keyCount    the number of keys in the array
     * @param keyLength   the length of each string key
     * @param keyLocality if the key is local/remote/random
     * @param instance    the HazelcastInstance that is used for keyLocality
     * @return the created array of keys
     */
    public static String[] generateStringKeys(String prefix, int keyCount, int keyLength, KeyLocality keyLocality,
                                              HazelcastInstance instance) {
        Set<Integer> targetPartitions = getTargetPartitions(keyLocality, instance);
        PartitionService partitionService = instance.getPartitionService();

        Map<Integer, Set<String>> keysPerPartitionMap = new HashMap<Integer, Set<String>>();
        for (Integer partitionId : targetPartitions) {
            keysPerPartitionMap.put(partitionId, new HashSet<String>());
        }

        int maxKeysPerPartition = (int) Math.ceil(keyCount / (float) targetPartitions.size());

        int generatedKeyCount = 0;
        for (; ; ) {
            String key = prefix + generateString(keyLength - prefix.length());
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

        return toArray(keyCount, keysPerPartitionMap);
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

    private static Set<Integer> getTargetPartitions(KeyLocality keyLocality, HazelcastInstance hz) {
        Set<Integer> targetPartitions = new HashSet<Integer>();
        PartitionService partitionService = hz.getPartitionService();
        Member localMember = getLocalMember(hz);
        switch (keyLocality) {
            case LOCAL:
                addLocalPartitions(targetPartitions, partitionService, localMember);
                break;
            case REMOTE:
                addRemotePartitions(targetPartitions, partitionService, localMember);
                break;
            case RANDOM:
                addAllPartitions(targetPartitions, partitionService);
                break;
            case SINGLE_PARTITION:
                targetPartitions.add(0);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized keyLocality:" + keyLocality);
        }
        return targetPartitions;
    }

    private static Member getLocalMember(HazelcastInstance hz) {
        try {
            return hz.getCluster().getLocalMember();
        } catch (UnsupportedOperationException ignore) {
            // clients throw UnsupportedOperationExceptions.
            return null;
        }
    }

    private static void addLocalPartitions(Set<Integer> partitions, PartitionService partitionService, Member localMember) {
        for (Partition partition : partitionService.getPartitions()) {
            if (localMember == null || localMember.equals(partition.getOwner())) {
                partitions.add(partition.getPartitionId());
            }
        }
    }

    private static void addRemotePartitions(Set<Integer> partitions, PartitionService partitionService, Member localMember) {
        for (Partition partition : partitionService.getPartitions()) {
            if (localMember == null || !localMember.equals(partition.getOwner())) {
                partitions.add(partition.getPartitionId());
            }
        }
    }

    private static void addAllPartitions(Set<Integer> partitions, PartitionService partitionService) {
        for (Partition partition : partitionService.getPartitions()) {
            partitions.add(partition.getPartitionId());
        }
    }

    private static String[] toArray(int keyCount, Map<Integer, Set<String>> keysPerPartitionMap) {
        String[] result = new String[keyCount];
        int index = 0;
        for (Set<String> keysPerPartition : keysPerPartitionMap.values()) {
            for (String string : keysPerPartition) {
                result[index] = string;
                index++;
            }
        }
        return result;
    }

    private interface Generator<K> {

        K newKey();

        K newConstantKey();
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
}
