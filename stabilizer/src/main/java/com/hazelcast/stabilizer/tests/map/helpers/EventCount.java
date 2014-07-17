package com.hazelcast.stabilizer.tests.map.helpers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class EventCount implements DataSerializable {
    public AtomicLong localAddCount = new AtomicLong(0);
    public  AtomicLong localRemoveCount = new AtomicLong(0);
    public  AtomicLong localUpdateCount = new AtomicLong(0);
    public  AtomicLong localEvictCount = new AtomicLong(0);
    public  AtomicLong localReplaceCount = new AtomicLong(0);

    public EventCount(){
    }

    public void add(EventCount c){
        localAddCount.addAndGet(c.localAddCount.get());
        localRemoveCount.addAndGet(c.localRemoveCount.get());
        localUpdateCount.addAndGet(c.localUpdateCount.get());
        localEvictCount.addAndGet(c.localEvictCount.get());
        localReplaceCount.addAndGet(c.localReplaceCount.get());
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(localAddCount);
        out.writeObject(localRemoveCount);
        out.writeObject(localUpdateCount);
        out.writeObject(localEvictCount);
        out.writeObject(localReplaceCount);
    }

    public void readData(ObjectDataInput in) throws IOException {
        localAddCount = in.readObject();
        localRemoveCount = in.readObject();
        localUpdateCount = in.readObject();
        localEvictCount = in.readObject();
        localReplaceCount = in.readObject();
    }

    public long total(){
        return  localAddCount.get() +
                localRemoveCount.get() +
                localUpdateCount.get() +
                localEvictCount.get() +
                localReplaceCount.get();
    }

    @Override
    public String toString() {
        return "Count{" +
                "putCount=" + localAddCount +
                ", putTransientCount=" + localRemoveCount +
                ", putIfAbsentCount=" + localUpdateCount +
                ", replaceCount=" + localEvictCount +
                ", getCount=" + localReplaceCount +
                '}';
    }
}