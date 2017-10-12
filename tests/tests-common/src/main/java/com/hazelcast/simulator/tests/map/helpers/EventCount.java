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

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static org.junit.Assert.assertEquals;

public class EventCount implements DataSerializable {

    public AtomicLong addCount = new AtomicLong(0);
    public AtomicLong removeCount = new AtomicLong(0);
    public AtomicLong updateCount = new AtomicLong(0);
    public AtomicLong evictCount = new AtomicLong(0);

    public EventCount() {
    }

    public void add(EventCount eventCount) {
        addCount.addAndGet(eventCount.addCount.get());
        removeCount.addAndGet(eventCount.removeCount.get());
        updateCount.addAndGet(eventCount.updateCount.get());
        evictCount.addAndGet(eventCount.evictCount.get());
    }

    public long calculateMapSize() {
        return addCount.get() - (evictCount.get() + removeCount.get());
    }

    public long calculateMapSize(EntryListenerImpl listener) {
        return listener.addCount.get() - (listener.evictCount.get() + listener.removeCount.get());
    }

    public void waitWhileListenerEventsIncrease(EntryListenerImpl listener, int maxIterationNoChange) {
        int iterationsWithoutChange = 0;
        long prev = 0;
        do {
            long diff = absDifference(listener);

            if (diff >= prev) {
                iterationsWithoutChange++;
            } else {
                iterationsWithoutChange = 0;
            }
            prev = diff;

            sleepSeconds(2);
        } while (!sameEventCount(listener) && iterationsWithoutChange < maxIterationNoChange);
    }

    public boolean sameEventCount(EntryListenerImpl listener) {
        return (absDifference(listener) == 0);
    }

    private long absDifference(EntryListenerImpl listener) {
        Long listenerTotal = listener.addCount.get()
                + listener.updateCount.get()
                + listener.removeCount.get()
                + listener.evictCount.get();

        return Math.abs(total() - listenerTotal);
    }

    private long total() {
        return addCount.get()
                + removeCount.get()
                + updateCount.get()
                + evictCount.get();
    }

    public void assertEventsEquals(EntryListenerImpl listener) {
        assertEquals("Add Events ", addCount.get(), listener.addCount.get());
        assertEquals("Update Events ", updateCount.get(), listener.updateCount.get());
        assertEquals("Remove Events ", removeCount.get(), listener.removeCount.get());
        assertEquals("Evict Events ", evictCount.get(), listener.evictCount.get());
        assertEquals("calculated Map size ", calculateMapSize(), calculateMapSize(listener));
    }

    @Override
    public String toString() {
        return "Count{"
                + "putCount=" + addCount
                + ", putTransientCount=" + removeCount
                + ", putIfAbsentCount=" + updateCount
                + ", replaceCount=" + evictCount
                + '}';
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
}
