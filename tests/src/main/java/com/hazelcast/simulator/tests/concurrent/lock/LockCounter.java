package com.hazelcast.simulator.tests.concurrent.lock;

import java.io.Serializable;

public class LockCounter implements Serializable {

    public long locked;
    public long increased;
    public long unlocked;

    public LockCounter() {
    }

    public void add(LockCounter counter) {
        locked += counter.locked;
        increased += counter.increased;
        unlocked += counter.unlocked;
    }

    @Override
    public String toString() {
        return "LockCounter{"
                + "locked=" + locked
                + ", inced=" + increased
                + ", unlocked=" + unlocked
                + '}';
    }
}
