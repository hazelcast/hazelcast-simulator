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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateString;
import static java.lang.String.format;

public final class KeyUtils {

    private static final boolean UNBALANCED = false;
    private static final boolean BALANCED = true;

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

    private static KeyGenerator<Integer> newIntKeyGenerator(HazelcastInstance hz, KeyLocality keyLocality, int keyCount) {
        switch (keyLocality) {
            case LOCAL:
                return new IntKeyGenerator(hz, keyLocality, keyCount, UNBALANCED);
            case LOCAL_BALANCED:
                return new IntKeyGenerator(hz, keyLocality, keyCount, BALANCED);
            case REMOTE:
                return new IntKeyGenerator(hz, keyLocality, keyCount, UNBALANCED);
            case REMOTE_BALANCED:
                return new IntKeyGenerator(hz, keyLocality, keyCount, BALANCED);
            case RANDOM:
                return new IntKeyGenerator(hz, keyLocality, keyCount, UNBALANCED);
            case RANDOM_BALANCED:
                return new IntKeyGenerator(hz, keyLocality, keyCount, BALANCED);
            case SINGLE_PARTITION:
                return new SinglePartitionIntKeyGenerator();
            default:
                return new SharedIntKeyGenerator();
        }
    }

    private static KeyGenerator<String> newStringKeyGenerator(
            HazelcastInstance hz, KeyLocality keyLocality, int keyCount, int keyLength, String prefix) {
        switch (keyLocality) {
            case LOCAL:
                return new StringKeyGenerator(hz, keyLocality, keyCount, keyLength, prefix, UNBALANCED);
            case LOCAL_BALANCED:
                return new StringKeyGenerator(hz, keyLocality, keyCount, keyLength, prefix, BALANCED);
            case REMOTE:
                return new StringKeyGenerator(hz, keyLocality, keyCount, keyLength, prefix, UNBALANCED);
            case REMOTE_BALANCED:
                return new StringKeyGenerator(hz, keyLocality, keyCount, keyLength, prefix, BALANCED);
            case RANDOM:
                return new StringKeyGenerator(hz, keyLocality, keyCount, keyLength, prefix, UNBALANCED);
            case RANDOM_BALANCED:
                return new StringKeyGenerator(hz, keyLocality, keyCount, keyLength, prefix, BALANCED);
            case SINGLE_PARTITION:
                return new SinglePartitionStringKeyGenerator(keyLength, prefix);
            default:
                return new SharedStringKeyGenerator(keyLength, prefix);
        }
    }

    /**
     * Generates an array of int keys with a configurable keyLocality.
     *
     * If the instance is a client, keyLocality is ignored.
     *
     * @param keyCount    the number of keys in the array
     * @param keyLocality if the key is local/remote/random
     * @param hz          the HazelcastInstance that is used for keyLocality
     * @return the created array of keys
     */
    public static int[] generateIntKeys(int keyCount, KeyLocality keyLocality, HazelcastInstance hz) {
        KeyGenerator<Integer> keyGenerator = newIntKeyGenerator(hz, keyLocality, keyCount);

        int[] keys = new int[keyCount];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = keyGenerator.next();
        }
        return keys;
    }

    /**
     * Generates an array of int keys with a configurable keyLocality.
     *
     * If the instance is a client, keyLocality is ignored.
     *
     * @param keyCount    the number of keys in the array
     * @param keyLocality if the key is local/remote/random
     * @param hz          the HazelcastInstance that is used for keyLocality
     * @return the created array of keys
     */
    public static Integer[] generateIntegerKeys(int keyCount, KeyLocality keyLocality, HazelcastInstance hz) {
        KeyGenerator<Integer> keyGenerator = newIntKeyGenerator(hz, keyLocality, keyCount);

        Integer[] keys = new Integer[keyCount];
        for (int i = 0; i < keys.length; i++) {
            keys[i] = keyGenerator.next();
        }
        return keys;
    }

    /**
     * Generates a string key with a configurable keyLocality.
     *
     * @param keyLength   the length of each string key
     * @param keyLocality if the key is local/remote/random
     * @param hz          the HazelcastInstance that is used for keyLocality
     * @return the created key
     */
    public static String generateStringKey(int keyLength, KeyLocality keyLocality, HazelcastInstance hz) {
        KeyGenerator<String> keyGenerator = newStringKeyGenerator(hz, keyLocality, Integer.MAX_VALUE, keyLength, "");
        return keyGenerator.next();
    }

    /**
     * Generates an array of string keys with a configurable keyLocality.
     *
     * If the instance is a client, keyLocality is ignored.
     *
     * @param keyCount    the number of keys in the array
     * @param keyLength   the length of each string key
     * @param keyLocality if the key is local/remote/random
     * @param hz          the HazelcastInstance that is used for keyLocality
     * @return the created array of keys
     */
    public static String[] generateStringKeys(int keyCount, int keyLength, KeyLocality keyLocality, HazelcastInstance hz) {
        return generateStringKeys("", keyCount, keyLength, keyLocality, hz);
    }

    /**
     * Generates an array of string keys with a configurable keyLocality.
     *
     * If the hz is a client, keyLocality is ignored.
     *
     * @param prefix      prefix for the generated keys
     * @param keyCount    the number of keys in the array
     * @param keyLocality if the key is local/remote/random
     * @param hz          the HazelcastInstance that is used for keyLocality
     * @return the created array of keys
     */
    public static String[] generateStringKeys(String prefix, int keyCount, KeyLocality keyLocality, HazelcastInstance hz) {
        int keyLength = (int) (prefix.length() + Math.ceil(Math.log10(keyCount))) + 2;
        return generateStringKeys(prefix, keyCount, keyLength, keyLocality, hz);
    }

    /**
     * Generates an array of string keys with a configurable keyLocality.
     *
     * If the hz is a client, keyLocality is ignored.
     *
     * @param prefix      prefix for the generated keys
     * @param keyCount    the number of keys in the array
     * @param keyLength   the length of each string key
     * @param keyLocality if the key is local/remote/random
     * @param hz          the HazelcastInstance that is used for keyLocality
     * @return the created array of keys
     */
    public static String[] generateStringKeys(String prefix, int keyCount, int keyLength, KeyLocality keyLocality,
                                              HazelcastInstance hz) {
        String[] keys = new String[keyCount];
        KeyGenerator<String> keyGenerator = newStringKeyGenerator(hz, keyLocality, keyCount, keyLength, prefix);
        for (int i = 0; i < keys.length; i++) {
            keys[i] = keyGenerator.next();
        }

        return keys;
    }

    interface KeyGenerator<K> {
        K next();
    }

    abstract static class AbstractKeyGenerator<K> implements KeyGenerator<K> {

        protected final Random random = new Random();
        protected final HazelcastInstance hz;
        protected final int keyCount;

        private final Set<K>[] keysPerPartition;
        private final PartitionService partitionService;
        private final int maxKeysPerPartition;
        private final KeyLocality keyLocality;
        private final boolean balanced;

        @SuppressWarnings("unchecked")
        AbstractKeyGenerator(HazelcastInstance hz, KeyLocality keyLocality, int keyCount, boolean balanced) {
            this.hz = hz;
            this.keyLocality = keyLocality;
            this.keyCount = keyCount;
            this.balanced = balanced;

            this.partitionService = hz.getPartitionService();

            Set<Integer> targetPartitions = getTargetPartitions();
            this.maxKeysPerPartition = (int) Math.ceil(keyCount / (float) targetPartitions.size());

            int partitionCount = partitionService.getPartitions().size();
            this.keysPerPartition = new Set[partitionCount];
            for (Integer partitionId : targetPartitions) {
                keysPerPartition[partitionId] = new HashSet<K>();
            }
        }

        @Override
        public final K next() {
            for (; ; ) {
                K key = generateKey();

                Partition partition = partitionService.getPartition(key);

                Set<K> keys = keysPerPartition[partition.getPartitionId()];
                if (keys == null) {
                    continue;
                }

                if (keys.contains(key)) {
                    continue;
                }

                if (balanced && keys.size() == maxKeysPerPartition) {
                    continue;
                }

                keys.add(key);
                return key;
            }
        }

        protected abstract K generateKey();

        private Set<Integer> getTargetPartitions() {
            Set<Integer> targetPartitions = new HashSet<Integer>();
            Member localMember = getLocalMember(hz);
            switch (keyLocality) {
                case LOCAL:
                    getLocalPartitions(targetPartitions, localMember);
                    break;
                case LOCAL_BALANCED:
                    getLocalPartitions(targetPartitions, localMember);
                    break;
                case REMOTE:
                    getRemotePartitions(targetPartitions, localMember);
                    break;
                case REMOTE_BALANCED:
                    getRemotePartitions(targetPartitions, localMember);
                    break;
                case RANDOM:
                    getRandomPartitions(targetPartitions);
                    break;
                case RANDOM_BALANCED:
                    getRandomPartitions(targetPartitions);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported keyLocality: " + keyLocality);
            }
            return targetPartitions;
        }

        private void getRandomPartitions(Set<Integer> targetPartitions) {
            for (Partition partition : partitionService.getPartitions()) {
                targetPartitions.add(partition.getPartitionId());
            }
        }

        private void getRemotePartitions(Set<Integer> targetPartitions, Member localMember) {
            for (Partition partition : partitionService.getPartitions()) {
                if (localMember == null || !localMember.equals(partition.getOwner())) {
                    targetPartitions.add(partition.getPartitionId());
                }
            }
        }

        private void getLocalPartitions(Set<Integer> targetPartitions, Member localMember) {
            for (Partition partition : partitionService.getPartitions()) {
                if (localMember == null || localMember.equals(partition.getOwner())) {
                    targetPartitions.add(partition.getPartitionId());
                }
            }
        }

        private Member getLocalMember(HazelcastInstance hz) {
            try {
                return hz.getCluster().getLocalMember();
            } catch (UnsupportedOperationException ignore) {
                // clients throw UnsupportedOperationExceptions
                return null;
            }
        }
    }

    private static final class SharedIntKeyGenerator implements KeyGenerator<Integer> {

        private int current;

        @Override
        public Integer next() {
            int value = current;
            current++;
            return value;
        }
    }

    private static final class SinglePartitionIntKeyGenerator implements KeyGenerator<Integer> {

        @Override
        public Integer next() {
            return 0;
        }
    }

    private static final class IntKeyGenerator extends AbstractKeyGenerator<Integer> {

        private IntKeyGenerator(HazelcastInstance hz, KeyLocality keyLocality, int keyCount, boolean balanced) {
            super(hz, keyLocality, keyCount, balanced);
        }

        @Override
        protected Integer generateKey() {
            return random.nextInt(Integer.MAX_VALUE);
        }
    }

    private static final class StringKeyGenerator extends AbstractKeyGenerator<String> {

        private final int keyLength;
        private final String prefix;

        private StringKeyGenerator(
                HazelcastInstance hz, KeyLocality keyLocality, int keyCount, int keyLength, String prefix, boolean balanced) {
            super(hz, keyLocality, keyCount, balanced);
            this.keyLength = keyLength;
            this.prefix = prefix;
        }

        @Override
        protected String generateKey() {
            if (prefix.length() == 0) {
                return generateString(keyLength);
            } else {
                return prefix + generateString(keyLength - prefix.length());
            }
        }
    }


    private static final class SinglePartitionStringKeyGenerator implements KeyGenerator<String> {

        private final String key;

        SinglePartitionStringKeyGenerator(int keyLength, String prefix) {
            key = padWithZero(new StringBuilder(prefix), keyLength).toString();
        }

        @Override
        public String next() {
            return key;
        }
    }

    private static StringBuilder padWithZero(StringBuilder sb, int length) {
        int count = length - sb.length();

        for (int i = 0; i < count; i++) {
            sb.append('0');
        }
        return sb;
    }

    private static final class SharedStringKeyGenerator implements KeyGenerator<String> {

        private int current;
        private final int keyLength;
        private final String prefix;

        SharedStringKeyGenerator(int keyLength, String prefix) {
            this.keyLength = keyLength;
            this.prefix = prefix;
        }

        @Override
        public String next() {
            int id = current;
            current++;

            int x = keyLength - prefix.length();
            return prefix + format("%0" + x + "d", id);
        }
    }
}
