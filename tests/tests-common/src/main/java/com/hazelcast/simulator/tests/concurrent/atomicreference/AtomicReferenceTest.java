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
package com.hazelcast.simulator.tests.concurrent.atomicreference;

import com.hazelcast.core.IAtomicReference;
import com.hazelcast.simulator.test.BaseThreadContext;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.AbstractTest;
import com.hazelcast.simulator.tests.helpers.KeyLocality;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.KeyLocality.SHARED;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateString;

public class AtomicReferenceTest extends AbstractTest {

    // properties
    public int countersLength = 1000;
    public KeyLocality keyLocality = SHARED;
    public int valueCount = 1000;
    public int valueLength = 512;
    public boolean useStringValue = true;

    private IAtomicReference<Object>[] counters;
    private Object[] values;

    @Setup
    public void setup() {
        values = new Object[valueCount];
        Random random = new Random();
        for (int i = 0; i < valueCount; i++) {
            if (useStringValue) {
                values[i] = generateString(valueLength);
            } else {
                values[i] = generateByteArray(random, valueLength);
            }
        }

        counters = new IAtomicReference[countersLength];
        String[] names = generateStringKeys(name, countersLength, keyLocality, targetInstance);
        for (int i = 0; i < counters.length; i++) {
            IAtomicReference<Object> atomicReference = targetInstance.getAtomicReference(names[i]);
            atomicReference.set(values[random.nextInt(values.length)]);
            counters[i] = atomicReference;
        }
    }

    @TimeStep(prob = 1)
    public void put(ThreadContext context) {
        Object value = values[context.randomInt(values.length)];
        context.randomReference().set(value);
    }

    @TimeStep(prob = 0)
    public void get(ThreadContext context) {
        context.randomReference().get();
    }

    public class ThreadContext extends BaseThreadContext {
        private IAtomicReference<Object> randomReference() {
            return counters[randomInt(counters.length)];
        }
    }

    @Teardown
    public void teardown() {
        for (IAtomicReference counter : counters) {
            counter.destroy();
        }
    }


}
