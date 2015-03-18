package com.hazelcast.simulator.tests.concurrent.lock;

import java.io.Serializable;

public class LockCounter implements Serializable {

    public long locked = 0;
    public long inced = 0;
    public long unlocked = 0;

    public LockCounter() {
    }

    public void add(LockCounter c) {
        locked += c.locked;
        inced += c.inced;
        unlocked += c.unlocked;
    }

    @Override
    public String toString() {
        return "LockCounter{" +
                "locked=" + locked +
                ", inced=" + inced +
                ", unlocked=" + unlocked +
                '}';
    }
}