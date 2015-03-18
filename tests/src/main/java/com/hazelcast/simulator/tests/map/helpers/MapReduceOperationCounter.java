package com.hazelcast.simulator.tests.map.helpers;

import java.io.Serializable;

public class MapReduceOperationCounter implements Serializable {
    public long mapReduce = 0;
    public long getMapEntry = 0;
    public long modifyMapEntry = 0;

    public void add(MapReduceOperationCounter operationCounter) {
        mapReduce += operationCounter.mapReduce;
        getMapEntry += operationCounter.getMapEntry;
        modifyMapEntry += operationCounter.modifyMapEntry;
    }

    @Override
    public String toString() {
        return "MapReduceOperationCounter{"
                + "mapReduce=" + mapReduce
                + ", getMapEntry=" + getMapEntry
                + ", modifyMapEntry=" + modifyMapEntry
                + '}';
    }
}
