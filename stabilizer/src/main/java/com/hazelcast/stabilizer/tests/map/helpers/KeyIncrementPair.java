package com.hazelcast.stabilizer.tests.map.helpers;

import java.io.Serializable;
import java.util.Random;

/**
 * Holds a key and an amount to increment by.
 * Also useful for printing data in nice format.
 */
public class KeyIncrementPair implements Serializable {

    public final int key;
    public final int inc;

    public KeyIncrementPair(Random random, int maxKey, int maxInc) {

        key = random.nextInt(maxKey);
        inc = random.nextInt(maxInc-1)+1;
    }

    @Override
    public String toString() {
        return "{" +
                "key=" + key +
                ", inc=" + inc +
                '}';
    }
}