package com.hazelcast.simulator.tests.map.helpers;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapEvent;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;

public class EntryListenerImpl<K, V> implements EntryListener<K, V>, DataSerializable {

    private final Random random = new Random();

    public AtomicLong addCount = new AtomicLong();
    public AtomicLong removeCount = new AtomicLong();
    public AtomicLong updateCount = new AtomicLong();
    public AtomicLong evictCount = new AtomicLong();

    public int minDelayMs;
    public int maxDelayMs;

    @SuppressWarnings("unused")
    public EntryListenerImpl() {
    }

    public EntryListenerImpl(int minDelayMs, int maxDelayMs) {
        if (minDelayMs < 0 || maxDelayMs < 0) {
            throw new IllegalArgumentException("minDelayMS and maxDelayMS must be greater or equal 0!");
        }

        this.minDelayMs = minDelayMs;
        this.maxDelayMs = maxDelayMs;
    }

    @Override
    public void entryAdded(EntryEvent<K, V> objectObjectEntryEvent) {
        delay();
        addCount.incrementAndGet();
    }

    @Override
    public void entryRemoved(EntryEvent<K, V> objectObjectEntryEvent) {
        delay();
        removeCount.incrementAndGet();
    }

    @Override
    public void entryUpdated(EntryEvent<K, V> objectObjectEntryEvent) {
        delay();
        updateCount.incrementAndGet();
    }

    @Override
    public void entryEvicted(EntryEvent<K, V> objectObjectEntryEvent) {
        delay();
        evictCount.incrementAndGet();
    }

    @Override
    public void mapEvicted(MapEvent mapEvent) {
    }

    @Override
    public void mapCleared(MapEvent mapEvent) {
    }

    private void delay() {
        if (maxDelayMs > 0) {
            sleepMillis(minDelayMs + random.nextInt(maxDelayMs));
        }
    }

    @Override
    public String toString() {
        return "EntryCounter{"
                + "addCount=" + addCount
                + ", removeCount=" + removeCount
                + ", updateCount=" + updateCount
                + ", evictCount=" + evictCount
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof EntryListenerImpl)) {
            return false;
        }

        EntryListenerImpl that = (EntryListenerImpl) o;
        return addCount.get() == that.addCount.get() &&
                evictCount.get() == that.evictCount.get() &&
                removeCount.get() == that.removeCount.get() &&
                updateCount.get() == that.updateCount.get();
    }

    @Override
    public int hashCode() {
        int result = addCount != null ? addCount.hashCode() : 0;
        result = 31 * result + (removeCount != null ? removeCount.hashCode() : 0);
        result = 31 * result + (updateCount != null ? updateCount.hashCode() : 0);
        result = 31 * result + (evictCount != null ? evictCount.hashCode() : 0);
        return result;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(addCount);
        out.writeObject(removeCount);
        out.writeObject(updateCount);
        out.writeObject(evictCount);

        out.writeInt(minDelayMs);
        out.writeInt(maxDelayMs);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        addCount = in.readObject();
        removeCount = in.readObject();
        updateCount = in.readObject();
        evictCount = in.readObject();

        minDelayMs = in.readInt();
        maxDelayMs = in.readInt();
    }
}
