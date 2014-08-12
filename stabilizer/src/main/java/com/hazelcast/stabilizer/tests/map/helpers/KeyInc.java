package com.hazelcast.stabilizer.tests.map.helpers;

import java.io.Serializable;

public class KeyInc implements Serializable {

    public int key=0;
    public int inc=0;

    public KeyInc() {
    }


    @Override
    public String toString() {
        return "KeyInc{" +
                "key=" + key +
                ", inc=" + inc +
                '}';
    }
}