package com.hazelcast.simulator.tests.map.prunability;

import java.io.Serializable;

public class KeyPojo implements Serializable {
    int a;
    String b;
    long c;

    public KeyPojo() {
    }

    public KeyPojo(int a, String b, long c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public int getA() {
        return a;
    }

    public String getB() {
        return b;
    }

    public long getC() {
        return c;
    }
}