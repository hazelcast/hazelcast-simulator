package com.hazelcast.stabilizer.tests.map.helpers;

import java.io.Serializable;


public class OppCounterIdxTest implements Serializable {
    public long predicateBuilderCount=0;
    public long sqlStringCount=0;
    public long pagePredCount=0;
    public long updateEmployeCount=0;
    public long destroyCount =0;

    public OppCounterIdxTest(){

    }

    @Override
    public String toString() {
        return "OppCounter{" +
                "predicateBuilderCount=" + predicateBuilderCount +
                ", sqlStringCount=" + sqlStringCount +
                ", pagePredCount=" + pagePredCount +
                ", updateEmployeCount=" + updateEmployeCount +
                ", destroyCount=" + destroyCount +
                '}';
    }

    public void add(OppCounterIdxTest o){
        predicateBuilderCount += o.predicateBuilderCount;
        sqlStringCount += o.sqlStringCount;
        pagePredCount += o.pagePredCount;
        updateEmployeCount += o.updateEmployeCount;
        destroyCount += o.destroyCount;
    }
}