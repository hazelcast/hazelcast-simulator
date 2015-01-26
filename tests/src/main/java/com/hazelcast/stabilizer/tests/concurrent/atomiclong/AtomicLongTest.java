/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer.tests.concurrent.atomiclong;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.tests.helpers.KeyLocality;
import com.hazelcast.stabilizer.tests.helpers.KeyUtils;
import com.hazelcast.stabilizer.tests.StabilizerAbstractTest;

import static org.junit.Assert.assertEquals;

public class AtomicLongTest extends StabilizerAbstractTest {

    private static final String KEY_PREFIX = "AtomicLongTest";

    // Properties
    public int countersLength = 1000;
    public String basename = "atomicLong";
    public KeyLocality keyLocality = KeyLocality.Random;
    public double writeProb = 1.0;

    private IAtomicLong[] counters;
    private String serviceName;

    @Override
    protected void afterSetup(HazelcastInstance hazelcastInstance) throws Exception {
        counters = new IAtomicLong[countersLength];
        for (int i = 0; i < counters.length; i++) {
            String key = KEY_PREFIX + KeyUtils.generateStringKey(8, keyLocality, hazelcastInstance);
            counters[i] = hazelcastInstance.getAtomicLong(key);
        }
        serviceName = counters[0].getServiceName();

        addOperation(BaseOperation.PUT, writeProb);
        addOperationRemainingProbability(BaseOperation.GET);
    }

    @Override
    protected void beforeTeardown() throws Exception {
        for (IAtomicLong counter : counters) {
            counter.destroy();
        }
    }

    @Override
    protected void doVerify(HazelcastInstance hazelcastInstance, long verifyCounter) throws Exception {
        long actual = 0;
        for (DistributedObject distributedObject : hazelcastInstance.getDistributedObjects()) {
            String key = distributedObject.getName();
            if ((distributedObject.getServiceName().equals(serviceName) && key.startsWith(KEY_PREFIX))) {
                actual += hazelcastInstance.getAtomicLong(key).get();
            }
        }

        assertEquals(verifyCounter, actual);
    }

    @Override
    protected BaseWorker createWorker() {
        return new Worker();
    }

    private class Worker extends BaseWorker {
        @Override
        protected void doRun(BaseOperation baseOperation) {
            IAtomicLong counter = getRandomCounter();

            switch (baseOperation) {
                case PUT:
                    incrementVerifyCounter();
                    counter.incrementAndGet();
                    break;
                case GET:
                    counter.get();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private IAtomicLong getRandomCounter() {
            return counters[getRandomInt(counters.length)];
        }
    }

    public static void main(String[] args) throws Throwable {
        AtomicLongTest test = new AtomicLongTest();
        new TestRunner<AtomicLongTest>(test).withDuration(10).run();
    }
}
