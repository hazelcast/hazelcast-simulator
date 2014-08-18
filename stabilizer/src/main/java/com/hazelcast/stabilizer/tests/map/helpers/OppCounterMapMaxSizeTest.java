package com.hazelcast.stabilizer.tests.map.helpers;

import java.io.Serializable;


public class OppCounterMapMaxSizeTest implements Serializable {
    public long put=0;
    public long putAsync=0;
    public long get=0;
    public long verified=0;

    public OppCounterMapMaxSizeTest(){
    }


    @Override
    public String toString() {
        return "OppCounterMapMaxSizeTest{" +
                "put=" + put +
                ", putAsync=" + putAsync +
                ", get=" + get +
                ", verified=" + verified +
                '}';
    }

    public void add(OppCounterMapMaxSizeTest o){
        put += o.put;
        putAsync += o.putAsync;
        get += o.get;
        verified += o.verified;
    }
}