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
package com.hazelcast.simulator.ignite;

import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.apache.ignite.IgniteAtomicLong;

public class AtomicLongTest extends IgniteTest {

    // properties
    // public KeyLocality keyLocality = SHARED;
    public int countersLength = 1000;

    private IgniteAtomicLong totalCounter;
    private IgniteAtomicLong[] counters;

    @Setup
    public void setup() {
        totalCounter = ignite.atomicLong(name + ":TotalCounter", 0, true);
        counters = new IgniteAtomicLong[countersLength];

        //String[] names = generateStringKeys(name, countersLength, keyLocality, targetInstance);
        for (int i = 0; i < countersLength; i++) {

            counters[i] = ignite.atomicLong("" + i, 0, true);
        }
    }

    @TimeStep(prob = -1)
    public void get(ThreadState state) {
        state.randomCounter().get();
    }

    @TimeStep(prob = 0.1)
    public void write(ThreadState state) {
        state.randomCounter().incrementAndGet();
        state.increments++;
    }

    public class ThreadState extends BaseThreadState {

        private long increments;

        private IgniteAtomicLong randomCounter() {
            int index = randomInt(counters.length);
            return counters[index];
        }
    }

    @Teardown
    public void teardown() {
        for (IgniteAtomicLong counter : counters) {
            counter.close();
        }
        totalCounter.close();
    }

}
