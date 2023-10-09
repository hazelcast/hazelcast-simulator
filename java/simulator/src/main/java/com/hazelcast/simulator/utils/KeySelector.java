package com.hazelcast.simulator.utils;

import java.util.function.IntToLongFunction;

/**
 * Interface of the long key selection algorithm to be used for selecting
 * the next long key the test to operate with.
 */
public interface KeySelector {
    /**
     * Retrieves the next long key with the provided random function
     * used to randomize the next keys.
     *
     * @param randomFn The random function.
     * @return the next long key to operate with
     */
    long nextKey(IntToLongFunction randomFn);
}
