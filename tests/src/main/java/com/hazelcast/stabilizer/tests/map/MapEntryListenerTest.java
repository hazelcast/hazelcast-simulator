/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.helpers.StringUtils;
import com.hazelcast.stabilizer.tests.map.helpers.EntryListenerImpl;
import com.hazelcast.stabilizer.tests.map.helpers.EventCount;
import com.hazelcast.stabilizer.tests.map.helpers.ScrambledZipfianGenerator;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Teardown;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;

import java.util.Random;

import static com.hazelcast.stabilizer.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;

/**
 * This test is using a map to generate map entry Events,  we use an EntryListener implementation to count the received
 * events.  The event we are generation and counting are:  add  remove  update  evict
 * as currently the event system of hazelcast is on a "best effort" basis  it is possible that  the number of generated
 * events for will not equals the number of events received, In the future the hz event system could change.  for now
 * we can say the number of events received can not be greater that an the number of events generated
 */
public class MapEntryListenerTest {

    private static final int SLEEP_MS_CATCH_EVENTS = 8000;

    private final static ILogger log = Logger.getLogger(MapEntryListenerTest.class);

    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int valueLength = 100;
    public int keyCount = 1000;
    public int valueCount = 1000;
    public boolean randomDistributionUniform = false;
    public int maxEntryListenerDelayMs = 0;
    public int minEntryListenerDelayMs = 0;

    //check these add up to 1
    public double writeProb = 0.4;
    public double evictProb = 0.2;
    public double removeProb = 0.2;
    public double deleteProb = 0.2;

    //check these add up to 1   (writeProb is split up into sub styles)
    public double writeUsingPutProb = 0.5;
    public double writeUsingPutIfAbsent = 0.25;
    public double replaceProb = 0.25;

    private String[] values;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private EntryListenerImpl listener;
    private ScrambledZipfianGenerator kesyZipfian = new ScrambledZipfianGenerator(keyCount);
    private IMap<Object, Object> map;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        values = StringUtils.generateStrings(valueCount, valueLength);

        map = targetInstance.getMap(basename);
        listener = new EntryListenerImpl(minEntryListenerDelayMs, maxEntryListenerDelayMs);
        map.addEntryListener(listener, true);
    }

    @Warmup(global = true)
    public void globalWarmup() {
        IMap map = targetInstance.getMap(basename);

        EventCount initCounter = new EventCount();

        int v = 0;
        for (int k = 0; k < keyCount; k++) {
            map.put(k, values[v]);
            initCounter.localAddCount.getAndIncrement();
            v = (v + 1 == values.length ? 0 : v + 1);
        }
        IList results = targetInstance.getList(basename + "eventCount");
        results.add(initCounter);
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();

        // wait, so that our entry listener implementation can catch the last incoming events from other members / clients
        sleepMillis(SLEEP_MS_CATCH_EVENTS);

        IList listeners = targetInstance.getList(basename + "listeners");
        listeners.add(listener);
    }

    @Teardown(global = true)
    public void tearDown() throws Exception {
        IMap map = targetInstance.getMap(basename);
        map.destroy();
    }

    private class Worker implements Runnable {
        private EventCount eventCount = new EventCount();
        private final Random random = new Random();
        int key;

        public void run() {
            while (!testContext.isStopped()) {

                if (randomDistributionUniform) {
                    key = random.nextInt(keyCount);
                } else {
                    key = kesyZipfian.nextInt();
                }

                double chance = random.nextDouble();
                if (chance < writeProb) {
                    final Object value = values[random.nextInt(values.length)];

                    chance = random.nextDouble();
                    if (chance < writeUsingPutProb) {
                        map.lock(key);
                        try {
                            if (map.containsKey(key)) {
                                eventCount.localUpdateCount.getAndIncrement();
                            } else {
                                eventCount.localAddCount.getAndIncrement();
                            }
                            map.put(key, value);
                        } finally {
                            map.unlock(key);
                        }
                    } else if (chance < writeUsingPutIfAbsent + writeUsingPutProb) {
                        map.lock(key);
                        try {
                            if (map.putIfAbsent(key, value) == null) {
                                eventCount.localAddCount.getAndIncrement();
                            }
                        } finally {
                            map.unlock(key);
                        }
                    } else if (chance < replaceProb + writeUsingPutIfAbsent + writeUsingPutProb) {
                        Object orig = map.get(key);
                        if (orig != null && map.replace(key, orig, value)) {
                            eventCount.localUpdateCount.getAndIncrement();
                        }
                    }
                } else if (chance < evictProb + writeProb) {
                    map.lock(key);
                    try {
                        if (map.containsKey(key)) {
                            eventCount.localEvictCount.getAndIncrement();
                        }
                        map.evict(key);
                    } finally {
                        map.unlock(key);
                    }
                } else if (chance < removeProb + evictProb + writeProb) {
                    Object o = map.remove(key);
                    if (o != null) {
                        eventCount.localRemoveCount.getAndIncrement();
                    }
                } else if (chance < deleteProb + removeProb + evictProb + writeProb) {
                    map.lock(key);
                    try {
                        if (map.containsKey(key)) {
                            eventCount.localRemoveCount.getAndIncrement();
                        }
                        map.delete(key);
                    } finally {
                        map.unlock(key);
                    }
                }
            }
            IList results = targetInstance.getList(basename + "eventCount");
            results.add(eventCount);
        }
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {
        IList<EntryListenerImpl> listeners = targetInstance.getList(basename + "listeners");
        for (int i = 0; i < listeners.size() - 1; i++) {
            EntryListenerImpl a = listeners.get(i);
            EntryListenerImpl b = listeners.get(i + 1);
            assertEquals(basename + ": not same amount of event in all listeners", a, b);
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        IList<EventCount> eventCounts = targetInstance.getList(basename + "eventCount");
        EventCount total = new EventCount();
        for (EventCount c : eventCounts) {
            total.add(c);
        }
        total.waiteWhileListenerEventsIncrease(listener, 10);

        log.info(basename + ": add = " + total.localAddCount.get() + " " + listener.addCount.get());
        log.info(basename + ": update = " + total.localUpdateCount.get() + " " + listener.updateCount.get());
        log.info(basename + ": remove = " + total.localRemoveCount.get() + " " + listener.removeCount.get());
        log.info(basename + ": evict = " + total.localEvictCount.get() + " " + listener.evictCount.get());

        log.info(basename + ": mapSZ = " + map.size() + " " + total.calculateMapSize() + " " + total.calculateMapSize(listener));

        total.assertEventsEquals(listener);
    }
}
