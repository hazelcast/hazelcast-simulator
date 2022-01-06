/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.hazelcast.simulator.tests.map.helpers;

import java.util.Random;

/**
 * Utility functions for zipfian generators.
 */
@SuppressWarnings("unused")
final class ZipfianUtils {

    private static final int FNV_OFFSET_BASIS_32 = 0x811c9dc5;
    private static final int FNV_PRIME_32 = 16777619;

    private static final long FNV_OFFSET_BASIS_64 = 0xCBF29CE484222325L;
    private static final long FNV_PRIME_64 = 1099511628211L;

    private static final Random RANDOM = new Random();
    private static final ThreadLocal<Random> THREAD_LOCAL_RANDOM = new ThreadLocal<>();

    private ZipfianUtils() {
    }

    public static Random random() {
        Random random = THREAD_LOCAL_RANDOM.get();
        if (random == null) {
            random = new Random(RANDOM.nextLong());
            THREAD_LOCAL_RANDOM.set(random);
        }
        return random;
    }

    /**
     * Generate a random ASCII string of a given length.
     */
    public static String generateASCIIString(int length) {
        int interval = '~' - ' ' + 1;

        byte[] buffer = new byte[length];
        random().nextBytes(buffer);
        for (int i = 0; i < length; i++) {
            if (buffer[i] < 0) {
                buffer[i] = (byte) ((-buffer[i] % interval) + ' ');
            } else {
                buffer[i] = (byte) ((buffer[i] % interval) + ' ');
            }
        }
        return new String(buffer);
    }

    /**
     * Hash an integer value.
     */
    public static long hash(long val) {
        return hashFNV64(val);
    }

    /**
     * 32 bit FNV hash. Produces more "random" hashes than (say) String.hashCode().
     *
     * @param val The value to hash.
     * @return The hash value
     */
    public static int hashFNV32(int val) {
        // from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
        int hashVal = FNV_OFFSET_BASIS_32;

        for (int i = 0; i < 4; i++) {
            int octet = val & 0x00ff;
            val = val >> 8;

            hashVal = hashVal ^ octet;
            hashVal = hashVal * FNV_PRIME_32;
        }
        return Math.abs(hashVal);
    }

    /**
     * 64 bit FNV hash. Produces more "random" hashes than (say) String.hashCode().
     *
     * @param val The value to hash.
     * @return The hash value
     */
    public static long hashFNV64(long val) {
        // from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
        long hashVal = FNV_OFFSET_BASIS_64;

        for (int i = 0; i < 8; i++) {
            long octet = val & 0x00ff;
            val = val >> 8;

            hashVal = hashVal ^ octet;
            hashVal = hashVal * FNV_PRIME_64;
        }
        return Math.abs(hashVal);
    }
}
