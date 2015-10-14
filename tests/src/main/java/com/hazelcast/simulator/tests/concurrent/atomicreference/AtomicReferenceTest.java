/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.concurrent.atomicreference;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateString;

public class AtomicReferenceTest {

    private static final ILogger LOGGER = Logger.getLogger(AtomicReferenceTest.class);

    private enum Operation {
        PUT,
        GET
    }

    // properties
    public String basename = AtomicReferenceTest.class.getSimpleName();
    public int countersLength = 1000;
    public KeyLocality keyLocality = KeyLocality.RANDOM;
    public int valueCount = 1000;
    public int valueLength = 512;
    public boolean useStringValue = true;
    public double putProb = 1.0;

    private final OperationSelectorBuilder<Operation> builder = new OperationSelectorBuilder<Operation>();

    private HazelcastInstance targetInstance;
    private IAtomicReference<Object>[] counters;
    private Object[] values;

    @Setup
    public void setup(TestContext context) throws Exception {
        targetInstance = context.getTargetInstance();

        values = new Object[valueCount];
        Random random = new Random();
        for (int i = 0; i < valueCount; i++) {
            if (useStringValue) {
                values[i] = generateString(valueLength);
            } else {
                values[i] = generateByteArray(random, valueLength);
            }
        }

        counters = getCounters();
        String[] names = generateStringKeys(basename + "-" + context.getTestId(), countersLength, keyLocality, targetInstance);
        for (int i = 0; i < counters.length; i++) {
            IAtomicReference<Object> atomicReference = targetInstance.getAtomicReference(names[i]);
            atomicReference.set(values[random.nextInt(values.length)]);
            counters[i] = atomicReference;
        }

        builder.addOperation(Operation.PUT, putProb)
                .addDefaultOperation(Operation.GET);
    }

    @SuppressWarnings("unchecked")
    private IAtomicReference<Object>[] getCounters() {
        return (IAtomicReference<Object>[]) new IAtomicReference[countersLength];
    }

    @Teardown
    public void teardown() throws Exception {
        for (IAtomicReference counter : counters) {
            counter.destroy();
        }
        LOGGER.info(getOperationCountInformation(targetInstance));
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        public Worker() {
            super(builder);
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            IAtomicReference<Object> counter = getRandomCounter();
            switch (operation) {
                case PUT:
                    Object value = values[randomInt(values.length)];
                    counter.set(value);
                    break;
                case GET:
                    counter.get();
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown operation: " + operation);
            }
        }

        private IAtomicReference<Object> getRandomCounter() {
            return counters[randomInt(counters.length)];
        }
    }

    public static void main(String[] args) throws Exception {
        AtomicReferenceTest test = new AtomicReferenceTest();
        new TestRunner<AtomicReferenceTest>(test).withDuration(10).run();
    }
}
