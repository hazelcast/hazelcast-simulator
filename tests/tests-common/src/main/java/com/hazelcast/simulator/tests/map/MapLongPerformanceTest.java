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

import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.AbstractTest;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

public class MapLongPerformanceTest extends AbstractTest {

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public int keyCount = 1000000;
    public double writeProb = 0.1;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private IMap<Integer, Long> map;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(basename);

        operationSelectorBuilder
                .addOperation(Operation.PUT, writeProb)
                .addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }

    @Warmup(global = true)
    public void warmup() {
        Streamer<Integer, Long> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < keyCount; i++) {
            streamer.pushEntry(i, 0L);
        }
        streamer.await();
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        public void timeStep(Operation operation) {
            Integer key = randomInt(keyCount);

            switch (operation) {
                case PUT:
                    map.set(key, System.currentTimeMillis());
                    break;
                case GET:
                    map.get(key);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }
}
