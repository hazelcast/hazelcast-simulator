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
package com.hazelcast.simulator.utils;

/**
 * Indicates if a key should be:
 * <ol>
 * <li>LOCAL: random generated local keys (perfectly balanced)</li>
 * <li>REMOTE: random generated remote keys (perfectly balanced)</li>
 * <li>RANDOM: random generated keys (perfectly balanced)</li>
 * <li>SHARED: random generated keys (same sequence on all Workers)</li>
 * <li>SINGLE_PARTITION: constant key for hitting a single partition</li>
 * </ol>
 */
public enum KeyLocality {

    /**
     * Generates random generated local keys (keys that point to locally owned partitions).
     *
     * The keys will be perfectly balanced over the partitions.
     */
    LOCAL,

    /**
     * Generates random generated remote keys.
     *
     * Each worker will generate it own random set of keys, so it is very unlikely they are going to be shared.
     *
     * The keys will be perfectly balanced over remote partitions.
     */
    REMOTE,

    /**
     * Generates random generated keys.
     *
     * Each worker will generate it own random set of keys, so it is very unlikely they are going to be shared. Also becase
     * each worker generates its own keys, the total size of key domain is 'load-generating-worker * keycount'.
     *
     * If you want to pound the same keys by all members, use {@link #SHARED}.
     *
     * The keys will be perfectly balanced over the partitions.
     */
    RANDOM,

    /**
     * Generates random generated keys (same sequence on all Workers).
     *
     * In doubt, this is the keyLocality you want to use. All keys will be shared by all members.
     *
     * Since all keys are shared, the total size of the key domain is equal to the keyCount configured on the member.
     */
    SHARED,

    /**
     * Generates a constant key for hitting a single partition
     */
    SINGLE_PARTITION,
}
