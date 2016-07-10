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

import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.metronome.MetronomeType;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorkerWithMultipleProbes;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateStrings;
import static com.hazelcast.simulator.worker.metronome.MetronomeFactory.withFixedIntervalMs;

public class ReplicatedMapTest extends AbstractTest {

    private enum Operation {
        PUT,
        REMOVE,
        GET
    }

    // properties
    public int keyCount = 10000;
    public int valueCount = 1;
    public int valueLength = 10;
    public MetronomeType metronomeType = MetronomeType.SLEEPING;
    public int intervalMs = 20;

    public double putProb = 0.45;
    public double getProb = 0.45;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private ReplicatedMap<Integer, String> map;

    private String[] values;

    @Setup
    public void setUp() throws Exception {
        map = targetInstance.getReplicatedMap(name + "-" + testContext.getTestId());

        operationSelectorBuilder.addOperation(Operation.PUT, putProb)
                .addOperation(Operation.GET, getProb)
                .addDefaultOperation(Operation.REMOVE);
    }

    @Warmup
    public void warmup() throws InterruptedException {
        values = generateStrings(valueCount, valueLength);
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorkerWithMultipleProbes<Operation> {

        private final Metronome metronome = withFixedIntervalMs(intervalMs, metronomeType);

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void timeStep(Operation operation, Probe probe) {
            metronome.waitForNext();
            int key = randomInt(keyCount);
            long started;

            switch (operation) {
                case PUT:
                    String value = randomValue();
                    started = System.nanoTime();
                    map.put(key, value);
                    probe.done(started);
                    break;
                case GET:
                    started = System.nanoTime();
                    map.get(key);
                    probe.done(started);
                    break;
                case REMOVE:
                    started = System.nanoTime();
                    map.remove(key);
                    probe.done(started);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private String randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown
    public void tearDown() throws Exception {
        map.destroy();
        logger.info(getOperationCountInformation(targetInstance));
    }

}
