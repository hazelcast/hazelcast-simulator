/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.map.helpers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class PredicateOperationCounter implements DataSerializable {

    public long predicateBuilderCount;
    public long sqlStringCount;
    public long pagePredicateCount;
    public long updateEmployeeCount;
    public long destroyCount;

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
