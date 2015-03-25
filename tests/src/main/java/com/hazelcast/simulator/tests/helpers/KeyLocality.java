package com.hazelcast.simulator.tests.helpers;

/**
 * Indicates if a key should be:
 * <ol>
 *     <li>LOCAL: so the key will be stored on the member</li>
 *     <li>REMOTE: if the key should be stored remote</li>
 *     <li>RANDOM: if you don't care where the key is stored</li>
 *     <li>SINGLE_PARTITION: if all traffic should go to a single partition</li>
 * </ol>
 */
public enum KeyLocality {
    LOCAL,
    REMOTE,
    RANDOM,
    SINGLE_PARTITION
}
