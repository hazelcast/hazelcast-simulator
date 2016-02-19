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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getPartitionDistributionInformation;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static org.junit.Assert.assertEquals;

public class AtomicLongTest {

    private static final ILogger LOGGER = Logger.getLogger(AtomicLongTest.class);

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public String basename = AtomicLongTest.class.getSimpleName();
    public KeyLocality keyLocality = KeyLocality.SHARED;
    public int countersLength = 1000;
    public int warmupIterations = 100;

    public double writeProb = 1.0;

    private final OperationSelectorBuilder<Operation> builder = new OperationSelectorBuilder<Operation>();

    private HazelcastInstance targetInstance;
    private IAtomicLong totalCounter;
    private IAtomicLong[] counters;

    @Setup
    public void setup(TestContext testContext) {
        targetInstance = testContext.getTargetInstance();

        totalCounter = targetInstance.getAtomicLong(basename + ":TotalCounter");
        counters = new IAtomicLong[countersLength];

        String[] names = generateStringKeys(basename, countersLength, keyLocality, testContext.getTargetInstance());
        for (int i = 0; i < countersLength; i++) {
            counters[i] = targetInstance.getAtomicLong(names[i]);
        }

        builder.addOperation(Operation.PUT, writeProb)
                .addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void teardown() {
        for (IAtomicLong counter : counters) {
            counter.destroy();
        }
        totalCounter.destroy();
        LOGGER.info(getOperationCountInformation(targetInstance));
        LOGGER.info(getPartitionDistributionInformation(targetInstance));
    }

    @Warmup
    public void warmup() {
        for (int i = 0; i < warmupIterations; i++) {
            for (IAtomicLong counter : counters) {
                counter.get();
            }
        }
    }

    @Verify
    public void verify() {
        String serviceName = totalCounter.getServiceName();
        String totalName = totalCounter.getName();

        long actual = 0;
        for (DistributedObject distributedObject : targetInstance.getDistributedObjects()) {
            String key = distributedObject.getName();
            if (serviceName.equals(distributedObject.getServiceName()) && key.startsWith(basename) && !key.equals(totalName)) {
                actual += targetInstance.getAtomicLong(key).get();
            }
        }

        assertEquals(totalCounter.get(), actual);
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        private int increments;

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
        protected void afterRun() {
            totalCounter.addAndGet(increments);
        }

        private IAtomicLong getRandomCounter() {
            int index = randomInt(counters.length);
            return counters[index];
        }
    }

    public static void main(String[] args) throws Exception {
        AtomicLongTest test = new AtomicLongTest();
        new TestRunner<AtomicLongTest>(test).withDuration(10).run();
    }
}
