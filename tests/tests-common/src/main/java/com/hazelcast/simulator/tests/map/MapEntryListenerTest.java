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
package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.map.helpers.EntryListenerImpl;
import com.hazelcast.simulator.tests.map.helpers.EventCount;
import com.hazelcast.simulator.tests.map.helpers.ScrambledZipfianGenerator;
import com.hazelcast.simulator.worker.selector.OperationSelector;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateStrings;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

/**
 * This test is using a map to generate map entry events. We use an {@link com.hazelcast.core.EntryListener} implementation to
 * count the received events. We are generating and counting add, remove, update and evict events.
 *
 * As currently the event system of Hazelcast is on a "best effort" basis, it is possible that the number of generated events will
 * not equal the number of events received. In the future the Hazelcast event system could change. For now we can say the number
 * of events received can not be greater than the number of events generated.
 */
public class MapEntryListenerTest extends AbstractTest {

    private enum MapOperation {
        PUT,
        EVICT,
        REMOVE,
        DELETE
    }

    private enum MapPutOperation {
        PUT,
        PUT_IF_ABSENT,
        REPLACE
    }

    private static final int SLEEP_CATCH_EVENTS_MILLIS = 8000;

    // properties
    public int valueLength = 100;
    public int keyCount = 1000;
    public int valueCount = 1000;
    public boolean randomDistributionUniform = false;
    public int maxEntryListenerDelayMs = 0;
    public int minEntryListenerDelayMs = 0;

    public double putProb = 0.4;
    public double evictProb = 0.2;
    public double removeProb = 0.2;
    public double deleteProb = 0.2;

    public double putUsingPutIfAbsentProb = 0.25;
    public double putUsingReplaceProb = 0.25;

    private final ScrambledZipfianGenerator keysZipfian = new ScrambledZipfianGenerator(keyCount);
    private final OperationSelectorBuilder<MapOperation> mapOperationSelectorBuilder
            = new OperationSelectorBuilder<MapOperation>();
    private final OperationSelectorBuilder<MapPutOperation> mapPutOperationSelectorBuilder
            = new OperationSelectorBuilder<MapPutOperation>();

    private String[] values;
    private EntryListenerImpl<Integer, String> listener;
    private IList<EventCount> eventCounts;
    private IList<EntryListenerImpl<Integer, String>> listeners;
    private IMap<Integer, String> map;

    @Setup
    public void setUp() {
        values = generateStrings(valueCount, valueLength);
        listener = new EntryListenerImpl<Integer, String>(minEntryListenerDelayMs, maxEntryListenerDelayMs);

        eventCounts = targetInstance.getList(name + "eventCount");
        listeners = targetInstance.getList(name + "listeners");

        map = targetInstance.getMap(name);
        map.addEntryListener(listener, true);

        mapOperationSelectorBuilder
                .addOperation(MapOperation.PUT, putProb)
                .addOperation(MapOperation.EVICT, evictProb)
                .addOperation(MapOperation.REMOVE, removeProb)
                .addOperation(MapOperation.DELETE, deleteProb);

        mapPutOperationSelectorBuilder
                .addOperation(MapPutOperation.PUT_IF_ABSENT, putUsingPutIfAbsentProb)
                .addOperation(MapPutOperation.REPLACE, putUsingReplaceProb)
                .addDefaultOperation(MapPutOperation.PUT);
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

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<MapOperation> {

        private final EventCount eventCount = new EventCount();
        private final OperationSelector<MapPutOperation> mapPutSelector = mapPutOperationSelectorBuilder.build();

        public Worker() {
            super(mapOperationSelectorBuilder);
        }

        @Override
        protected void timeStep(MapOperation mapOperation) {
            int key;

            if (randomDistributionUniform) {
                key = randomInt(keyCount);
            } else {
                key = keysZipfian.nextInt();
            }

            switch (mapOperation) {
                case PUT:
                    putOperation(key);
                    break;
                case EVICT:
                    map.lock(key);
                    try {
                        if (map.containsKey(key)) {
                            eventCount.evictCount.getAndIncrement();
                        }
                        map.evict(key);
                    } finally {
                        map.unlock(key);
                    }
                    break;
                case REMOVE:
                    String oldValue = map.remove(key);
                    if (oldValue != null) {
                        eventCount.removeCount.getAndIncrement();
                    }
                    break;
                case DELETE:
                    map.lock(key);
                    try {
                        if (map.containsKey(key)) {
                            eventCount.removeCount.getAndIncrement();
                        }
                        map.delete(key);
                    } finally {
                        map.unlock(key);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private void putOperation(int key) {
            String value = values[randomInt(values.length)];

            switch (mapPutSelector.select()) {
                case PUT:
                    map.lock(key);
                    try {
                        if (map.containsKey(key)) {
                            eventCount.updateCount.getAndIncrement();
                        } else {
                            eventCount.addCount.getAndIncrement();
                        }
                        map.put(key, value);
                    } finally {
                        map.unlock(key);
                    }
                    break;
                case PUT_IF_ABSENT:
                    map.lock(key);
                    try {
                        if (map.putIfAbsent(key, value) == null) {
                            eventCount.addCount.getAndIncrement();
                        }
                    } finally {
                        map.unlock(key);
                    }
                    break;
                case REPLACE:
                    String oldValue = map.get(key);
                    if (oldValue != null && map.replace(key, oldValue, value)) {
                        eventCount.updateCount.getAndIncrement();
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        public void afterRun() {
            eventCounts.add(eventCount);
        }

        @Override
        public void afterCompletion() {
            // wait, so that our entry listener implementation can catch the last incoming events from other members / clients
            sleepMillis(SLEEP_CATCH_EVENTS_MILLIS);

            listeners.add(listener);
        }
    }

    @Verify(global = true)
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
