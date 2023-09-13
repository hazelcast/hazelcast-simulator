package com.hazelcast.simulator.tests.map.prunability;

import java.io.Serializable;

public class KeyPojo implements Serializable {
    public int a;
    public String b;
    public int x;

    public KeyPojo() {
    }

    public KeyPojo(int a, String b, int x) {
        this.a = a;
        this.b = b;
        this.x = x;
    }

    public int getA() {
        return a;
    }

    public String getB() {
        return b;
    }

    public int getX() {
        return x;
    }

    @Override
    public String toString() {
        return "KeyPojo{" +
                "a=" + a +
                ", b='" + b + '\'' +
                ", x=" + x +
                '}';
    }
}