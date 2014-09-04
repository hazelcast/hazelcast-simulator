package com.hazelcast.stabilizer.tests.map.helpers;

import java.io.Serializable;


public class MapReduceCounter implements Serializable {
    public long mapReduce = 0;
    public long modifiedDataSet = 0;


    public MapReduceCounter() {
    }


    public void add(MapReduceCounter o) {
        mapReduce += o.mapReduce;
        modifiedDataSet += o.modifiedDataSet;
    }

    @Override
    public String toString() {
        return "MapReduceCounter{" +
                "mapReduce=" + mapReduce +
                ", modifiedDataSet=" + modifiedDataSet +
                '}';
    }
}