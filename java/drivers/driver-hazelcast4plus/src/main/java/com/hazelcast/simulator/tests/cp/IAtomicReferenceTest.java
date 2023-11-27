/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.cp;

import com.hazelcast.core.IFunction;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.GeneratorUtils;

public class IAtomicReferenceTest extends HazelcastTest {
    public int keyValueSizeKb = 1;
    private IAtomicReference<String> atomicReference;

    private String v; // this is always the value of [atomicReference]; before + after, irrespective of the op

    @Setup
    public void setup() {
        String kv = createString(keyValueSizeKb);
        v = kv;
        atomicReference = targetInstance.getCPSubsystem().getAtomicReference(kv);
        atomicReference.set(v);
    }

    String createString(int kb) {
        int bytes = kb * 1024;
        return GeneratorUtils.generateAsciiString(bytes);
    }

    @TimeStep(prob = 1)
    public void set(ThreadState state) {
        atomicReference.set(atomicReference.getName());
    }

    @TimeStep(prob = 0)
    public void alter(ThreadState state) {
        atomicReference.alter(state.identity);
    }

    @TimeStep(prob = 0)
    public boolean cas(ThreadState state) {
        return atomicReference.compareAndSet(v, v);
    }

    @TimeStep(prob = 0)
    public void casOptimisticConcurrencyControl(ThreadState state) {
        String observed;
        String newValue;
        do {
            observed = atomicReference.get(); // because we're modelling the pattern -- we know it's [v]...
            newValue = observed;
        } while (!atomicReference.compareAndSet(observed, newValue));
    }

    public class ThreadState extends BaseThreadState {
        final IFunction<String, String> identity = s -> s;
    }
}
