package com.hazelcast.simulator.tests.icache.helpers;

import java.io.Serializable;

public class ICacheReadWriteCounter implements Serializable {

    public long put;
    public long get;
    public long remove;

    public void add(ICacheReadWriteCounter c) {
        put += c.put;
        get += c.get;
        remove += c.remove;
    }

    public String toString() {
        return "ReadWriteICacheCounter{"
                + "put=" + put
                + ", get=" + get
                + ", remove=" + remove
                + '}';
    }
}
