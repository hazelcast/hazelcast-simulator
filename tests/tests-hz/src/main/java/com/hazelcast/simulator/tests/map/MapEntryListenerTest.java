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
package com.hazelcast.simulator.tests.map;

import com.hazelcast.collection.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.map.helpers.EntryListenerImpl;
import com.hazelcast.simulator.tests.map.helpers.EventCount;
import com.hazelcast.simulator.tests.map.helpers.ScrambledZipfianGenerator;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateAsciiStrings;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

/**
 * This test is using a map to generate map entry events. We use an {@link com.hazelcast.core.EntryListener}
 * implementation to
 * count the received events. We are generating and counting add, remove, update and evict events.
 *
 * As currently the event system of Hazelcast is on a "best effort" basis, it is possible that the number of
 * generated events will not equal the number of events received. In the future the Hazelcast event system
 * could change. For now we can say the number of events received can not be greater than the number of events generated.
 */
public class MapEntryListenerTest extends HazelcastTest {

    private static final int SLEEP_CATCH_EVENTS_MILLIS = 8000;

    // properties
    public int valueLength = 100;
    public int keyCount = 1000;
    public int valueCount = 1000;
    public boolean randomDistributionUniform = false;
    public int maxEntryListenerDelayMs = 0;
    public int minEntryListenerDelayMs = 0;
    public int threadCount;

    private final ScrambledZipfianGenerator keysZipfian = new ScrambledZipfianGenerator(keyCount);

    private String[] values;
    private EntryListenerImpl<Integer, String> listener;
    private IList<EventCount> eventCounts;
    private IList<EntryListenerImpl<Integer, String>> listeners;
    private IMap<Integer, String> map;
    private final AtomicInteger threadsRemaining = new AtomicInteger();

    @Setup
    public void setUp() {
        values = generateAsciiStrings(valueCount, valueLength);
        listener = new EntryListenerImpl<Integer, String>(minEntryListenerDelayMs, maxEntryListenerDelayMs);

        eventCounts = targetInstance.getList(name + "eventCount");
        listeners = targetInstance.getList(name + "listeners");

        map = targetInstance.getMap(name);
        map.addEntryListener(listener, true);

        threadsRemaining.set(threadCount);
    }

    @Prepare(global = true)
    public void globalPrepare() {
        EventCount initCounter = new EventCount();

        for (int i = 0; i < keyCount; i++) {
            map.put(i, values[i % valueLength]);
            initCounter.addCount.getAndIncrement();
        }

        eventCounts.add(initCounter);
    }

    @TimeStep(prob = -1)
    public void put(ThreadState state) {
        int key = state.randomKey();
        map.lock(key);
        try {
            if (map.containsKey(key)) {
                state.eventCount.updateCount.getAndIncrement();
            } else {
                state.eventCount.addCount.getAndIncrement();
            }
            map.put(key, state.randomValue());
        } finally {
            map.unlock(key);
        }
    }

    @TimeStep(prob = 0.1)
    public void putIfAbsent(ThreadState state) {
        int key = state.randomKey();
        map.lock(key);
        try {
            if (map.putIfAbsent(key, state.randomValue()) == null) {
                state.eventCount.addCount.getAndIncrement();
            }
        } finally {
            map.unlock(key);
        }
    }

    @TimeStep(prob = 0.1)
    public void replace(ThreadState state) {
        int key = state.randomKey();
        String oldValue = map.get(key);
        if (oldValue != null && map.replace(key, oldValue, state.randomValue())) {
            state.eventCount.updateCount.getAndIncrement();
        }
    }

    @TimeStep(prob = 0.2)
    public void evict(ThreadState state) {
        int key = state.randomKey();
        map.lock(key);
        try {
            if (map.containsKey(key)) {
                state.eventCount.evictCount.getAndIncrement();
            }
            map.evict(key);
        } finally {
            map.unlock(key);
        }
    }

    @TimeStep(prob = 0.2)
    public void remove(ThreadState state) {
        int key = state.randomKey();
        String oldValue = map.remove(key);
        if (oldValue != null) {
            state.eventCount.removeCount.getAndIncrement();
        }
    }

    @TimeStep(prob = 0.2)
    public void delete(ThreadState state) {
        int key = state.randomKey();
        map.lock(key);
        try {
            if (map.containsKey(key)) {
                state.eventCount.removeCount.getAndIncrement();
            }
            map.delete(key);
        } finally {
            map.unlock(key);
        }
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        eventCounts.add(state.eventCount);


        if (threadsRemaining.decrementAndGet() == 0) {
            sleepSeconds(SLEEP_CATCH_EVENTS_MILLIS);

            listeners.add(listener);
        }
    }

    public class ThreadState extends BaseThreadState {

        private final EventCount eventCount = new EventCount();

        String randomValue() {
            return values[randomInt(values.length)];
        }

        int randomKey() {
            if (randomDistributionUniform) {
                return randomInt(keyCount);
            } else {
                return keysZipfian.nextInt();
            }
        }
    }

    @Verify
    public void globalVerify() {
        for (int i = 0; i < listeners.size() - 1; i++) {
            EntryListenerImpl a = listeners.get(i);
            EntryListenerImpl b = listeners.get(i + 1);
            assertEquals(name + ": not same amount of event in all listeners", a, b);
        }
    }

    @Verify(global = false)
    public void verify() {
        EventCount total = new EventCount();
        for (EventCount eventCount : eventCounts) {
            total.add(eventCount);
        }
        total.waitWhileListenerEventsIncrease(listener, 10);

        logger.info(format("Event counter for %s (actual / expected)"
                        + "%n add: %d / %d"
                        + "%n update: %d / %d"
                        + "%n remove: %d / %d"
                        + "%n evict: %d / %d"
                        + "%n mapSize: %d / %d",
                name,
                listener.addCount.get(), total.addCount.get(),
                listener.updateCount.get(), total.updateCount.get(),
                listener.removeCount.get(), total.removeCount.get(),
                listener.evictCount.get(), total.evictCount.get(),
                total.calculateMapSize(listener), total.calculateMapSize()
        ));

        total.assertEventsEquals(listener);
    }

    @Teardown(global = true)
    public void tearDown() {
        map.destroy();
    }

}
