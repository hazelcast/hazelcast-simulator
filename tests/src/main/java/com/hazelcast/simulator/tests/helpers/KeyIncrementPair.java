package com.hazelcast.simulator.tests.helpers;

import java.io.Serializable;
import java.util.Random;

/**
 * Helper class, holds a key and an amount to increment by.
 * Also useful for printing out data in nice format.
 */
public class KeyIncrementPair implements Serializable {

    public final int key;
    public final int increment;

    public KeyIncrementPair(Random random, int maxKey, int maxIncrement) {
        key = random.nextInt(maxKey);
        increment = random.nextInt(maxIncrement - 1) + 1;
    }

    @Override
    public String toString() {
        return "KeyIncrementPair{"
                + "key=" + key
                + ", increment=" + increment
                + '}';
    }
}
