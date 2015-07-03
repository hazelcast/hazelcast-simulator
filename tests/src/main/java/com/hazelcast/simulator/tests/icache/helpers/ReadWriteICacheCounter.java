package com.hazelcast.simulator.tests.icache.helpers;

import java.io.Serializable;

public class ReadWriteICacheCounter implements Serializable {

    public long put = 0;
    public long get = 0;
    public long remove = 0;

    public void add(ReadWriteICacheCounter c) {
        put += c.put;
        get += c.get;
        remove += c.remove;
    }

    public String toString() {
        return "Counter{"
                + "put=" + put
                + ", get=" + get
                + ", remove=" + remove
                + '}';
    }
}
