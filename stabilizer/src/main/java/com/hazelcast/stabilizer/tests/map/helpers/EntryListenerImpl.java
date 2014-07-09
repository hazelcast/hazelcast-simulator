package com.hazelcast.stabilizer.tests.map.helpers;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class EntryListenerImpl implements DataSerializable, EntryListener<Object, Object> {

    public AtomicLong addCount = new AtomicLong();
    public AtomicLong removeCount = new AtomicLong();
    public AtomicLong updateCount = new AtomicLong();
    public AtomicLong evictCount = new AtomicLong();

    public EntryListenerImpl( ) { }

    @Override
    public void entryAdded(EntryEvent<Object, Object> objectObjectEntryEvent) {
        addCount.incrementAndGet();
    }

    @Override
    public void entryRemoved(EntryEvent<Object, Object> objectObjectEntryEvent) {
        removeCount.incrementAndGet();
    }

    @Override
    public void entryUpdated(EntryEvent<Object, Object> objectObjectEntryEvent) {
        updateCount.incrementAndGet();
    }

    @Override
    public void entryEvicted(EntryEvent<Object, Object> objectObjectEntryEvent) {
        evictCount.incrementAndGet();
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(addCount);
        out.writeObject(removeCount);
        out.writeObject(updateCount);
        out.writeObject(evictCount);
    }

    public void readData(ObjectDataInput in) throws IOException {
        addCount = in.readObject();
        removeCount = in.readObject();
        updateCount = in.readObject();
        evictCount = in.readObject();
    }

    @Override
    public String toString() {
        return "EntryCounter{" +
                "addCount=" + addCount +
                ", removeCount=" + removeCount +
                ", updateCount=" + updateCount +
                ", evictCount=" + evictCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (!(o instanceof EntryListenerImpl)) return false;

        EntryListenerImpl that = (EntryListenerImpl) o;

        if (addCount.get() == that.addCount.get() &&
            evictCount.get() == that.evictCount.get() &&
            removeCount.get() == that.removeCount.get() &&
            updateCount.get() == that.updateCount.get()){

            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int result = addCount != null ? addCount.hashCode() : 0;
        result = 31 * result + (removeCount != null ? removeCount.hashCode() : 0);
        result = 31 * result + (updateCount != null ? updateCount.hashCode() : 0);
        result = 31 * result + (evictCount != null ? evictCount.hashCode() : 0);
        return result;
    }
}