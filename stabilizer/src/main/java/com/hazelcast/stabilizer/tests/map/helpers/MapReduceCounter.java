package com.hazelcast.stabilizer.tests.map.helpers;

import java.io.Serializable;


public class MapReduceCounter implements Serializable {
    public long mapReduce = 0;
    public long getMapEntry=0;
    public long modifyMapEntry = 0;


    public MapReduceCounter() {
    }


    public void add(MapReduceCounter o) {
        mapReduce += o.mapReduce;
        getMapEntry += o.getMapEntry;
        modifyMapEntry += o.modifyMapEntry;
    }

    @Override
    public String toString() {
        return "MapReduceCounter{" +
                "mapReduce=" + mapReduce +
                ", getMapEntry=" + getMapEntry +
                ", modifyMapEntry=" + modifyMapEntry +
                '}';
    }
}