/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import java.util.concurrent.atomic.AtomicLong;

public class MapOperationCounter implements DataSerializable {

    public AtomicLong putCount = new AtomicLong(0);
    public AtomicLong putAsyncCount = new AtomicLong(0);

    public AtomicLong putTTLCount = new AtomicLong(0);
    public AtomicLong putAsyncTTLCount = new AtomicLong(0);

    public AtomicLong putTransientCount = new AtomicLong(0);
    public AtomicLong putIfAbsentCount = new AtomicLong(0);

    public AtomicLong replaceCount = new AtomicLong(0);

    public AtomicLong getCount = new AtomicLong(0);
    public AtomicLong getAsyncCount = new AtomicLong(0);

    public AtomicLong removeCount = new AtomicLong(0);
    public AtomicLong removeAsyncCount = new AtomicLong(0);

    public AtomicLong deleteCount = new AtomicLong(0);
    public AtomicLong destroyCount = new AtomicLong(0);

    public MapOperationCounter() {
    }

    public long getTotalNoOfOps() {
        return putCount.get() + putAsyncCount.get()
                + putTTLCount.get() + putAsyncTTLCount.get()
                + putTransientCount.get() + putIfAbsentCount.get()
                + replaceCount.get()
                + getCount.get() + getAsyncCount.get()
                + removeCount.get() + removeAsyncCount.get()
                + deleteCount.get() + destroyCount.get();
    }

    public void add(MapOperationCounter c) {
        putCount.addAndGet(c.putCount.get());
        putAsyncCount.addAndGet(c.putAsyncCount.get());

        putTTLCount.addAndGet(c.putTTLCount.get());
        putAsyncTTLCount.addAndGet(c.putAsyncTTLCount.get());

        putTransientCount.addAndGet(c.putTransientCount.get());
        putIfAbsentCount.addAndGet(c.putIfAbsentCount.get());

        replaceCount.addAndGet(c.replaceCount.get());

        getCount.addAndGet(c.getCount.get());
        getAsyncCount.addAndGet(c.getAsyncCount.get());

        removeCount.addAndGet(c.removeCount.get());
        removeAsyncCount.addAndGet(c.removeAsyncCount.get());

        deleteCount.addAndGet(c.deleteCount.get());
        destroyCount.addAndGet(c.destroyCount.get());
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(putCount);
        out.writeObject(putAsyncCount);

        out.writeObject(putTTLCount);
        out.writeObject(putAsyncTTLCount);

        out.writeObject(putTransientCount);
        out.writeObject(putIfAbsentCount);

        out.writeObject(replaceCount);

        out.writeObject(getCount);
        out.writeObject(getAsyncCount);

        out.writeObject(removeCount);
        out.writeObject(removeAsyncCount);

        out.writeObject(deleteCount);
        out.writeObject(destroyCount);
    }

    public void readData(ObjectDataInput in) throws IOException {
        putCount = in.readObject();
        putAsyncCount = in.readObject();

        putTTLCount = in.readObject();
        putAsyncTTLCount = in.readObject();

        putTransientCount = in.readObject();
        putIfAbsentCount = in.readObject();

        replaceCount = in.readObject();

        getCount = in.readObject();
        getAsyncCount = in.readObject();

        removeCount = in.readObject();
        removeAsyncCount = in.readObject();

        deleteCount = in.readObject();
        destroyCount = in.readObject();
    }

    @Override
    public String toString() {
        return "MapOperationCounter{"
                + "putCount=" + putCount
                + ", putAsyncCount=" + putAsyncCount
                + ", putTTLCount=" + putTTLCount
                + ", putAsyncTTLCount=" + putAsyncTTLCount
                + ", putTransientCount=" + putTransientCount
                + ", putIfAbsentCount=" + putIfAbsentCount
                + ", replaceCount=" + replaceCount
                + ", getCount=" + getCount
                + ", getAsyncCount=" + getAsyncCount
                + ", removeCount=" + removeCount
                + ", removeAsyncCount=" + removeAsyncCount
                + ", deleteCount=" + deleteCount
                + ", destroyCount=" + destroyCount
                + '}';
    }
}
