package com.hazelcast.stabilizer.tests.map.helpers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class EventCount implements DataSerializable {
    public AtomicLong localAddCount = new AtomicLong(0);
    public AtomicLong localRemoveCount = new AtomicLong(0);
    public AtomicLong localUpdateCount = new AtomicLong(0);
    public AtomicLong localEvictCount = new AtomicLong(0);

    public EventCount() {
    }

    public void add(EventCount c) {
        localAddCount.addAndGet(c.localAddCount.get());
        localRemoveCount.addAndGet(c.localRemoveCount.get());
        localUpdateCount.addAndGet(c.localUpdateCount.get());
        localEvictCount.addAndGet(c.localEvictCount.get());
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(localAddCount);
        out.writeObject(localRemoveCount);
        out.writeObject(localUpdateCount);
        out.writeObject(localEvictCount);
    }

    public void readData(ObjectDataInput in) throws IOException {
        localAddCount = in.readObject();
        localRemoveCount = in.readObject();
        localUpdateCount = in.readObject();
        localEvictCount = in.readObject();
    }

    public long total() {
        return localAddCount.get() +
                localRemoveCount.get() +
                localUpdateCount.get() +
                localEvictCount.get();
    }

    public long absDifference(EntryListenerImpl e) {
        long countTotal = total();

        Long listenerTotal = e.addCount.get() +
                e.updateCount.get() +
                e.removeCount.get() +
                e.evictCount.get();

        return Math.abs(countTotal - listenerTotal);
    }

    public boolean sameEventCount(EntryListenerImpl listener) {
        return absDifference(listener) == 0;
    }

    public void waiteWhileListenerEventsIncrease(EntryListenerImpl listener, int maxIterationNoChange) throws InterruptedException {
        int noChange = 0;
        long prev = 0;
        do {
            long diff = absDifference(listener);

            if (diff >= prev) {
                noChange++;
            } else {
                noChange = 0;
            }
            prev = diff;

            Thread.sleep(2000);

        } while (!sameEventCount(listener) && noChange < maxIterationNoChange);
    }

    public long calculateMapSize() {
        return localAddCount.get() - (localEvictCount.get() + localRemoveCount.get());
    }

    public long calculateMapSize(EntryListenerImpl listener) {
        return listener.addCount.get() - (listener.evictCount.get() + listener.removeCount.get());
    }

    public void assertEventsEquals(EntryListenerImpl listener) {
        assertEquals("Add Events ", localAddCount.get(), listener.addCount.get());
        assertEquals("Update Events ", localUpdateCount.get(), listener.updateCount.get());
        assertEquals("Remove Events ", localRemoveCount.get(), listener.removeCount.get());
        assertEquals("Evict Events ", localEvictCount.get(), listener.evictCount.get());
        assertEquals("calculated Map size", calculateMapSize(), calculateMapSize(listener));
    }

    @Override
    public String toString() {
        return "Count{"
                + "putCount=" + localAddCount
                + ", putTransientCount=" + localRemoveCount
                + ", putIfAbsentCount=" + localUpdateCount
                + ", replaceCount=" + localEvictCount
                + '}';
    }
}