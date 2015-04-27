package com.hazelcast.simulator.tests.concurrent.lock;

import java.io.Serializable;

public class LockCounter implements Serializable {

    public long locked;
    public long inced;
    public long unlocked;

    public LockCounter() {
    }

    public void add(LockCounter counter) {
        locked += counter.locked;
        inced += counter.inced;
        unlocked += counter.unlocked;
    }

    @Override
    public String toString() {
        return "LockCounter{"
                + "locked=" + locked
                + ", inced=" + inced
                + ", unlocked=" + unlocked
                + '}';
    }
}
