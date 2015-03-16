package com.hazelcast.simulator.tests.helpers;

import java.io.Serializable;
import java.util.Random;

/*
* Helper class,  holds a key and an amount to increment by
* also use full for printing out data in nice format
* */
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