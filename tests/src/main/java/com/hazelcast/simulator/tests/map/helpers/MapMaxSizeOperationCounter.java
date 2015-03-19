package com.hazelcast.simulator.tests.map.helpers;

import java.io.Serializable;

public class MapMaxSizeOperationCounter implements Serializable {
    public long put = 0;
    public long putAsync = 0;
    public long get = 0;
    public long verified = 0;

    public MapMaxSizeOperationCounter() {
    }

    public void add(MapMaxSizeOperationCounter o) {
        put += o.put;
        putAsync += o.putAsync;
        get += o.get;
        verified += o.verified;
    }

    @Override
    public String toString() {
        return "MapMaxSizeOperationCounter{"
                + "put=" + put
                + ", putAsync=" + putAsync
                + ", get=" + get
                + ", verified=" + verified
                + '}';
    }
}