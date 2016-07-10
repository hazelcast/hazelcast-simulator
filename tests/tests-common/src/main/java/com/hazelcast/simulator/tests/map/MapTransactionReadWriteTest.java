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
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorkerWithMultipleProbes;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;

public class MapTransactionReadWriteTest extends AbstractTest {

    enum Operation {
        PUT,
        GET
    }

    // properties
    public int keyLength = 10;
    public int valueLength = 10;
    public int keyCount = 10000;
    public int valueCount = 10000;
    public KeyLocality keyLocality = KeyLocality.SHARED;
    public int minNumberOfMembers = 0;

    public double putProb = 0.1;
    public boolean useSet = false;

    private final OperationSelectorBuilder<Operation> builder = new OperationSelectorBuilder<Operation>();

    private IMap<Integer, Integer> map;
    private int[] keys;

    @Setup
    public void setup() {
        map = targetInstance.getMap(name);

        builder.addOperation(Operation.PUT, putProb).addDefaultOperation(Operation.GET);
    }

    @Warmup(global = false)
    public void warmup() {
        waitClusterSize(logger, targetInstance, minNumberOfMembers);
        keys = generateIntKeys(keyCount, keyLocality, targetInstance);

        Random random = new Random();
        Streamer<Integer, Integer> streamer = StreamerFactory.getInstance(map);
        for (int key : keys) {
            int value = random.nextInt(Integer.MAX_VALUE);
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorkerWithMultipleProbes<Operation> {

        public Worker() {
            super(builder);
        }

        @Override
        public void timeStep(Operation operation, Probe probe) {
            final int key = randomKey();
            final int value = randomValue();
            long started;

            switch (operation) {
                case PUT:
                    started = System.nanoTime();
                    targetInstance.executeTransaction(new TransactionalTask<Object>() {
                        @Override
                        public Object execute(TransactionalTaskContext transactionalTaskContext) {
                            TransactionalMap<Integer, Integer> txMap = transactionalTaskContext.getMap(map.getName());
                            if (useSet) {
                                txMap.set(key, value);
                            } else {
                                txMap.put(key, value);
                            }
                            return null;
                        }
                    });
                    probe.done(started);
                    break;
                case GET:
                    started = System.nanoTime();
                    targetInstance.executeTransaction(new TransactionalTask<Object>() {
                        @Override
                        public Object execute(TransactionalTaskContext transactionalTaskContext) {
                            TransactionalMap<Integer, Integer> txMap = transactionalTaskContext.getMap(map.getName());
                            txMap.put(key, value);
                            return null;
                        }
                    });
                    probe.done(started);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private int randomKey() {
            int length = keys.length;
            return keys[randomInt(length)];
        }

        private int randomValue() {
            return randomInt(Integer.MAX_VALUE);
        }
    }

    @Teardown
    public void teardown() {
        map.destroy();
        logger.info(getOperationCountInformation(targetInstance));
    }
}
