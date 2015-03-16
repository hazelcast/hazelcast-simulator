package com.hazelcast.simulator.tests.map.helpers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class PredicateOperationCounter implements DataSerializable {
    public long predicateBuilderCount = 0;
    public long sqlStringCount = 0;
    public long pagePredicateCount = 0;
    public long updateEmployeeCount = 0;
    public long destroyCount = 0;

    public PredicateOperationCounter() {
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(predicateBuilderCount);
        out.writeLong(sqlStringCount);
        out.writeLong(pagePredicateCount);
        out.writeLong(updateEmployeeCount);
        out.writeLong(destroyCount);
    }

    public void readData(ObjectDataInput in) throws IOException {
        predicateBuilderCount = in.readLong();
        sqlStringCount = in.readLong();
        pagePredicateCount = in.readLong();
        updateEmployeeCount = in.readLong();
        destroyCount = in.readLong();
    }

    @Override
    public String toString() {
        return "PredicateOperationCounter{"
                + "predicateBuilderCount=" + predicateBuilderCount
                + ", sqlStringCount=" + sqlStringCount
                + ", pagePredicateCount=" + pagePredicateCount
                + ", updateEmployeeCount=" + updateEmployeeCount
                + ", destroyCount=" + destroyCount
                + '}';
    }

    public void add(PredicateOperationCounter o) {
        predicateBuilderCount += o.predicateBuilderCount;
        sqlStringCount += o.sqlStringCount;
        pagePredicateCount += o.pagePredicateCount;
        updateEmployeeCount += o.updateEmployeeCount;
        destroyCount += o.destroyCount;
    }
}