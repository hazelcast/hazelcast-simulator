package com.hazelcast.stabilizer.tests.map.helpers;

import java.io.Serializable;

/**
 * Holds a key and an amount to increment by.
 * Also useful for printing data in nice format.
 */
public class KeyInc implements Serializable {

    public int key = 0;
    public int inc = 0;

    public KeyInc() {
    }

    @Override
    public String toString() {
        return "{" +
                "key=" + key +
                ", inc=" + inc +
                '}';
    }
}