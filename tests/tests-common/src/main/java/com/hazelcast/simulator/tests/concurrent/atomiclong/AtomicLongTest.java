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
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadContext;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Reset;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyLocality;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.KeyLocality.SHARED;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static org.junit.Assert.assertEquals;

public class AtomicLongTest extends AbstractTest {

    // properties
    public KeyLocality keyLocality = SHARED;
    public int countersLength = 1000;

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
    }

    @TimeStep(prob = 0.9)
    public void get(ThreadContext context) {
        context.randomCounter().get();
    }

    @TimeStep(prob = 0.1)
    public void put(ThreadContext context) {
        context.randomCounter().incrementAndGet();
        context.increments++;
    }

    public class ThreadContext extends BaseThreadContext {
        private long increments;

        private IAtomicLong randomCounter() {
            int index = randomInt(counters.length);
            return counters[index];
        }
    }

    @AfterRun
    public void afterRun(ThreadContext context) {
        totalCounter.addAndGet(context.increments);
        operationsCounter.addAndGet(context.iteration());
    }

    @Reset
    public void globalReset(){
        for (IAtomicLong counter : counters) {
            counter.set(0);
        }
        totalCounter.set(0);
    }

    @Verify
    public void verify() {
        String serviceName = totalCounter.getServiceName();
        String totalName = totalCounter.getName();

        long actual = 0;
        for (DistributedObject distributedObject : targetInstance.getDistributedObjects()) {
            String key = distributedObject.getName();
            if (serviceName.equals(distributedObject.getServiceName())
                    && key.startsWith(name)
                    && !key.equals(totalName)) {
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
    }
}
