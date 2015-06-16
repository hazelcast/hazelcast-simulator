package com.hazelcast.simulator.tests.icache.helpers;

import java.io.Serializable;

public final class ICacheListenerOperationCounter implements Serializable {

    public long register;
    public long registerIllegalArgException;
    public long deRegister;
    public long put;
    public long get;

    public void add(ICacheListenerOperationCounter counter) {
        register += counter.register;
        registerIllegalArgException += counter.registerIllegalArgException;
        deRegister += counter.deRegister;
        put += counter.put;
        get += counter.get;
    }

    public String toString() {
        return "Counter{"
                + "register=" + register
                + ", registerIllegalArgException=" + registerIllegalArgException
                + ", deRegister=" + deRegister
                + ", put=" + put
                + ", get=" + get
                + '}';
    }
}
