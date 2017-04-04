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
package com.hazelcast.simulator.tests.replicatedmap;

import com.hazelcast.core.IList;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.map.helpers.MapOperationCounter;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;

public class ReplicatedTTL extends HazelcastTest {

    // properties
    public int keyCount = 100000;

    public int minTTLExpiryMs = 1;
    public int maxTTLExpiryMs = 1000;

    private ReplicatedMap<Integer, Integer> map;
    private IList<MapOperationCounter> results;

    @Setup
    public void setUp() {
        map = targetInstance.getReplicatedMap(name);
        results = targetInstance.getList(name + "report");
    }

    @TimeStep(prob = 0.7)
    public void putTTL(ThreadState state) {
        try {
            int key = state.randomInt(keyCount);
            int value = state.randomInt();
            int delayMs = minTTLExpiryMs;
            if (maxTTLExpiryMs > 0) {
                delayMs += state.randomInt(maxTTLExpiryMs);
            }
            map.put(key, value, delayMs, TimeUnit.MILLISECONDS);
            state.count.putTTLCount.incrementAndGet();
        } catch (DistributedObjectDestroyedException e) {
            EmptyStatement.ignore(e);
        }
    }

    @TimeStep(prob = 0.3)
    public void get(ThreadState state) {
        try {
            int key = state.randomInt(keyCount);
            map.get(key);
            state.count.getCount.incrementAndGet();
        } catch (DistributedObjectDestroyedException e) {
            EmptyStatement.ignore(e);
        }
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        results.add(state.count);
    }

    public class ThreadState extends BaseThreadState {
        private final MapOperationCounter count = new MapOperationCounter();
    }

    @Verify(global = false)
    public void localVerify() {
        MapOperationCounter total = new MapOperationCounter();
        for (MapOperationCounter counter : results) {
            total.add(counter);
        }
        logger.info(name + ": " + total + " total of " + results.size());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {

                logger.info(name + ": " + "assert map Size = " + map.size());
                assertEquals(name + ": Replicated Map should be empty, some TTL events are not processed", 0, map.size());
            }
        });
    }
}
