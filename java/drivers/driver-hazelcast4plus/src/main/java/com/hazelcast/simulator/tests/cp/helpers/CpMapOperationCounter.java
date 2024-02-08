/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.cp.helpers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class CpMapOperationCounter implements DataSerializable {

    public AtomicLong putCount = new AtomicLong(0);
    public AtomicLong putIfAbsentCount = new AtomicLong(0);
    public AtomicLong setCount = new AtomicLong(0);

    public AtomicLong casCount = new AtomicLong(0);

    public AtomicLong getCount = new AtomicLong(0);

    public AtomicLong removeCount = new AtomicLong(0);
    public AtomicLong deleteCount = new AtomicLong(0);

    public CpMapOperationCounter() {
    }

    public long getTotalNoOfOps() {
        return putCount.get() + setCount.get() + putIfAbsentCount.get()
                + casCount.get() + getCount.get()
                + removeCount.get() + deleteCount.get();
    }

    public void add(CpMapOperationCounter c) {
        putCount.addAndGet(c.putCount.get());
        putIfAbsentCount.addAndGet(c.putIfAbsentCount.get());
        setCount.addAndGet(c.setCount.get());

        casCount.addAndGet(c.casCount.get());

        getCount.addAndGet(c.getCount.get());

        removeCount.addAndGet(c.removeCount.get());
        deleteCount.addAndGet(c.deleteCount.get());
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(putCount);
        out.writeObject(putIfAbsentCount);
        out.writeObject(setCount);

        out.writeObject(casCount);

        out.writeObject(getCount);

        out.writeObject(removeCount);
        out.writeObject(deleteCount);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        putCount = in.readObject();
        putIfAbsentCount = in.readObject();
        setCount = in.readObject();

        casCount = in.readObject();

        getCount = in.readObject();

        removeCount = in.readObject();
        deleteCount = in.readObject();
    }

    @Override
    public String toString() {
        return "CPMapOperationCounter{"
                + "putCount=" + putCount
                + ", putIfAbsentCount=" + putIfAbsentCount
                + ", setCount=" + setCount
                + ", casCount=" + casCount
                + ", getCount=" + getCount
                + ", removeCount=" + removeCount
                + ", deleteCount=" + deleteCount
                + '}';
    }
}
