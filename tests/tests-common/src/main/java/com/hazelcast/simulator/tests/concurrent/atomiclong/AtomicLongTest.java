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
package com.hazelcast.simulator.tests.concurrent.atomiclong;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getPartitionDistributionInformation;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static org.junit.Assert.assertEquals;

public class AtomicLongTest extends AbstractTest {

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public KeyLocality keyLocality = KeyLocality.SHARED;
    public int countersLength = 1000;
    public int warmupIterations = 100;

    public double writeProb = 1.0;

    private final OperationSelectorBuilder<Operation> builder = new OperationSelectorBuilder<Operation>();

    private AtomicLong operationsCounter = new AtomicLong();
    private IAtomicLong totalCounter;
    private IAtomicLong[] counters;

    @Setup
    public void setup() {
        totalCounter = targetInstance.getAtomicLong(name + ":TotalCounter");
        counters = new IAtomicLong[countersLength];

        String[] names = generateStringKeys(name, countersLength, keyLocality, targetInstance);
        for (int i = 0; i < countersLength; i++) {
            counters[i] = targetInstance.getAtomicLong(names[i]);
        }

        builder.addOperation(Operation.PUT, writeProb)
                .addDefaultOperation(Operation.GET);
    }

    @Warmup
    public void warmup() {
        for (int i = 0; i < warmupIterations; i++) {
            for (IAtomicLong counter : counters) {
                counter.get();
            }
        }
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        private long increments;

        public Worker() {
            super(builder);
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            IAtomicLong counter = getRandomCounter();

            switch (operation) {
                case PUT:
                    increments++;
                    counter.incrementAndGet();
                    break;
                case GET:
                    counter.get();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        public void afterRun() {
            totalCounter.addAndGet(increments);
            operationsCounter.addAndGet(getIteration());
        }

        private IAtomicLong getRandomCounter() {
            int index = randomInt(counters.length);
            return counters[index];
        }
    }

    @Verify
    public void verify() {
        String serviceName = totalCounter.getServiceName();
        String totalName = totalCounter.getName();

        long actual = 0;
        for (DistributedObject distributedObject : targetInstance.getDistributedObjects()) {
            String key = distributedObject.getName();
            if (serviceName.equals(distributedObject.getServiceName()) && key.startsWith(name) && !key.equals(totalName)) {
                actual += targetInstance.getAtomicLong(key).get();
            }
        }

        assertEquals(totalCounter.get(), actual);
    }

    @Teardown
    public void teardown() {
        for (IAtomicLong counter : counters) {
            counter.destroy();
        }
        totalCounter.destroy();

        logger.info("Operations: " + operationsCounter);
        logger.info(getOperationCountInformation(targetInstance));
        logger.info(getPartitionDistributionInformation(targetInstance));
    }


}
