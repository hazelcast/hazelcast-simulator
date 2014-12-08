package com.hazelcast.stabilizer.tests.utils;

/**
 * Indicates if a key should be:
 * <ol>
 *     <li>local: so the key will be stored on the member</li>
 *     <li>remote: if the key should be stored remote</li>
 *     <li>random: if you don't care where the key is stored</li>
 *     <li>singlepartition: if all traffic should go to a single partition</li>
 * </ol>
 */
public enum KeyLocality {
    Local, Remote, Random, SinglePartition
}
