package com.hazelcast.simulator.tests.helpers;

/**
 * Indicates if a key should be:
 * <ol>
 *     <li>Local: so the key will be stored on the member</li>
 *     <li>Remote: if the key should be stored remote</li>
 *     <li>Random: if you don't care where the key is stored</li>
 *     <li>SinglePartition: if all traffic should go to a single partition</li>
 * </ol>
 */
public enum KeyLocality {
    Local, Remote, Random, SinglePartition
}
