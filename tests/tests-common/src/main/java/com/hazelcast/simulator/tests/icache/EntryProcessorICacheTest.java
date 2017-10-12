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
package com.hazelcast.simulator.tests.icache;

import com.hazelcast.core.IList;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static org.junit.Assert.assertEquals;

public class EntryProcessorICacheTest extends AbstractTest {

    // properties
    public int keyCount = 1000;
    public int minProcessorDelayMs = 0;
    public int maxProcessorDelayMs = 0;

    private IList<Map<Integer, Long>> resultsPerWorker;
    private Cache<Integer, Long> cache;

    @Setup
    public void setup() {
        resultsPerWorker = targetInstance.getList(name + ":ResultMap");

        CacheManager cacheManager = createCacheManager(targetInstance);
        cache = cacheManager.getCache(name);
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Integer, Long> streamer = StreamerFactory.getInstance(cache);
        for (int i = 0; i < keyCount; i++) {
            streamer.pushEntry(i, 0L);
        }
        streamer.await();
    }

    @BeforeRun
    public void beforeRun(ThreadState state) {
        for (int i = 0; i < keyCount; i++) {
            state.result.put(i, 0L);
        }
    }

    @TimeStep
    public void timeStep(ThreadState state) {
        int key = state.randomInt(keyCount);
        long increment = state.randomInt(100);

        int delayMs = 0;
        if (maxProcessorDelayMs != 0) {
            delayMs = minProcessorDelayMs + state.randomInt(maxProcessorDelayMs);
        }

        cache.invoke(key, new IncrementEntryProcessor(increment, delayMs));
        state.increment(key, increment);
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        // sleep to give time for the last EntryProcessor tasks to complete
        sleepMillis(maxProcessorDelayMs * 2);
        resultsPerWorker.add(state.result);
    }

    public class ThreadState extends BaseThreadState {

        private final Map<Integer, Long> result = new HashMap<Integer, Long>();

        private void increment(int key, long increment) {
            result.put(key, result.get(key) + increment);
        }
    }

    private static final class IncrementEntryProcessor implements EntryProcessor<Integer, Long, Object>, Serializable {

        private final long increment;
        private final int delayMs;

        private IncrementEntryProcessor(long increment, int delayMs) {
            this.increment = increment;
            this.delayMs = delayMs;
        }

        @Override
        public Object process(MutableEntry<Integer, Long> entry, Object... arguments) {
            if (delayMs > 0) {
                sleepMillis(delayMs);
            }
            long newValue = entry.getValue() + increment;
            entry.setValue(newValue);
            return null;
        }
    }

    @Verify
    public void verify() {
        long[] amount = new long[keyCount];

        for (Map<Integer, Long> map : resultsPerWorker) {
            for (Map.Entry<Integer, Long> entry : map.entrySet()) {
                amount[entry.getKey()] += entry.getValue();
            }
        }

        int failures = 0;
        for (int i = 0; i < keyCount; i++) {
            long expected = amount[i];
            long found = cache.get(i);
            if (expected != found) {
                failures++;
            }
        }

        assertEquals("Failures have been found", 0, failures);
    }

    @Teardown
    public void teardown() {
        cache.close();
        resultsPerWorker.destroy();
    }
}
