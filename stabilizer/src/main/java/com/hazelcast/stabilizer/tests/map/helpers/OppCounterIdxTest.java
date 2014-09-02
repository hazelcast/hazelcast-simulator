package com.hazelcast.stabilizer.tests.map.helpers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;


public class OppCounterIdxTest implements DataSerializable {
    public long predicateBuilderCount = 0;
    public long sqlStringCount = 0;
    public long pagePredCount = 0;
    public long updateEmployeCount = 0;
    public long destroyCount = 0;

    public OppCounterIdxTest() {

    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(predicateBuilderCount);
        out.writeLong(sqlStringCount);
        out.writeLong(pagePredCount);
        out.writeLong(updateEmployeCount);
        out.writeLong(destroyCount);
    }

    public void readData(ObjectDataInput in) throws IOException {
        predicateBuilderCount = in.readLong();
        sqlStringCount = in.readLong();
        pagePredCount = in.readLong();
        updateEmployeCount = in.readLong();
        destroyCount = in.readLong();
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

    public void add(OppCounterIdxTest o) {
        predicateBuilderCount += o.predicateBuilderCount;
        sqlStringCount += o.sqlStringCount;
        pagePredCount += o.pagePredCount;
        updateEmployeCount += o.updateEmployeCount;
        destroyCount += o.destroyCount;
    }
}