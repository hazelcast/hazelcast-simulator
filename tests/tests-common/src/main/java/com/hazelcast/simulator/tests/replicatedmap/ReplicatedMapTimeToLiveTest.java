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
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.AbstractTest;
import com.hazelcast.simulator.tests.map.helpers.MapOperationCounter;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.metronome.MetronomeType;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static com.hazelcast.simulator.worker.metronome.MetronomeFactory.withFixedIntervalMs;
import static org.junit.Assert.assertEquals;

public class ReplicatedMapTimeToLiveTest extends AbstractTest {

    private enum Operation {
        PUT_TTL,
        GET
    }

    // properties
    public int keyCount = 100000;

    public double putTTLProb = 0.7;
    public double getProb = 0.3;
    public MetronomeType metronomeType = MetronomeType.SLEEPING;
    public int intervalMs = 20;

    public int minTTLExpiryMs = 1;
    public int maxTTLExpiryMs = 1000;

    private final OperationSelectorBuilder<Operation> builder = new OperationSelectorBuilder<Operation>();

    private ReplicatedMap<Integer, Integer> map;
    private IList<MapOperationCounter> results;

    @Setup
    public void setup() {
        map = targetInstance.getReplicatedMap(name);
        results = targetInstance.getList(name + "report");

        builder.addOperation(Operation.PUT_TTL, putTTLProb)
                .addOperation(Operation.GET, getProb);
    }



    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {
        private final MapOperationCounter count = new MapOperationCounter();
        private final Metronome metronome = withFixedIntervalMs(intervalMs, metronomeType);


        public Worker() {
            super(builder);
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            metronome.waitForNext();
            try {
                int key = randomInt(keyCount);

                switch (operation) {
                    case PUT_TTL:
                        int value = randomInt();
                        int delayMs = minTTLExpiryMs;
                        if (maxTTLExpiryMs > 0) {
                            delayMs += randomInt(maxTTLExpiryMs);
                        }
                        map.put(key, value, delayMs, TimeUnit.MILLISECONDS);
                        count.putTTLCount.incrementAndGet();
                        break;
                    case GET:
                        map.get(key);
                        count.getCount.incrementAndGet();
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            } catch (DistributedObjectDestroyedException e) {
                EmptyStatement.ignore(e);
            }
        }

        @Override
        public void afterRun() {
            results.add(count);
        }
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
