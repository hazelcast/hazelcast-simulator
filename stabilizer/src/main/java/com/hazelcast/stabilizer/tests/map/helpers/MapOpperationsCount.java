package com.hazelcast.stabilizer.tests.map.helpers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class MapOpperationsCount implements DataSerializable {
    public AtomicLong putCount = new AtomicLong(0);
    public AtomicLong putTransientCount = new AtomicLong(0);
    public AtomicLong putIfAbsentCount = new AtomicLong(0);
    public AtomicLong replaceCount = new AtomicLong(0);
    public AtomicLong getCount = new AtomicLong(0);
    public AtomicLong getAsyncCount = new AtomicLong(0);
    public AtomicLong deleteCount = new AtomicLong(0);
    public AtomicLong destroyCount = new AtomicLong(0);

    public MapOpperationsCount(){}

    public void add(MapOpperationsCount c){
        putCount.addAndGet(c.putCount.get());
        putTransientCount.addAndGet(c.putTransientCount.get());
        putIfAbsentCount.addAndGet(c.putIfAbsentCount.get());
        replaceCount.addAndGet(c.replaceCount.get());
        getCount.addAndGet(c.getCount.get());
        getAsyncCount.addAndGet(c.getAsyncCount.get());
        deleteCount.addAndGet(c.deleteCount.get());
        destroyCount.addAndGet(c.destroyCount.get());
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(putCount);
        out.writeObject(putTransientCount);
        out.writeObject(putIfAbsentCount);
        out.writeObject(replaceCount);
        out.writeObject(getCount);
        out.writeObject(getAsyncCount);
        out.writeObject(deleteCount);
        out.writeObject(destroyCount);
    }

    public void readData(ObjectDataInput in) throws IOException {
        putCount = in.readObject();
        putTransientCount = in.readObject();
        putIfAbsentCount = in.readObject();
        replaceCount = in.readObject();
        getCount = in.readObject();
        getAsyncCount = in.readObject();
        deleteCount = in.readObject();
        destroyCount = in.readObject();
    }

    public long total(){
        return  putCount.get() +
                putTransientCount.get() +
                putIfAbsentCount.get() +
                replaceCount.get() +
                getCount.get() +
                getAsyncCount.get() +
                deleteCount.get() +
                destroyCount.get();
    }

    @Override
    public String toString() {
        return "MapOpperationsCount{" +
                "putCount=" + putCount +
                ", putTransientCount=" + putTransientCount +
                ", putIfAbsentCount=" + putIfAbsentCount +
                ", replaceCount=" + replaceCount +
                ", getCount=" + getCount +
                ", getAsyncCount=" + getAsyncCount +
                ", deleteCount=" + deleteCount +
                ", destroyCount=" + destroyCount +
                '}';
    }
}