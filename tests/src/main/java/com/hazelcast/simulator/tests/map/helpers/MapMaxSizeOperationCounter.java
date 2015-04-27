package com.hazelcast.simulator.tests.map.helpers;

import java.io.Serializable;

public class MapMaxSizeOperationCounter implements Serializable {

    public long put;
    public long putAsync;
    public long get;
    public long verified;

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
