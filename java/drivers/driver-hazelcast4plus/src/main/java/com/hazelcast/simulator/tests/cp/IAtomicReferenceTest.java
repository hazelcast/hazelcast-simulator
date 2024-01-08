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

import java.util.UUID;

public class IAtomicReferenceTest extends HazelcastTest {
    public int keySizeBytes = 128;
    public int valueSizeBytes = 1024;
    private IAtomicReference<String> atomicReference;

    private String v; // this is always the value of [atomicReference]; before + after, irrespective of the op

    @Setup
    public void setup() {
        String k = GeneratorUtils.generateAsciiString(keySizeBytes);
        v = GeneratorUtils.generateAsciiString(valueSizeBytes);
        atomicReference = targetInstance.getCPSubsystem().getAtomicReference(k);
        atomicReference.set(v);
    }

    @TimeStep(prob = 1)
    public void set(ThreadState state) {
        atomicReference.set(v);
    }

    @TimeStep(prob = 0)
    public void get(ThreadState state) {
        atomicReference.get();
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

    @TimeStep(prob = 0)
    public void createThenDelete(ThreadState state) {
        // there's no notion of a delete, therefore we destroy -- this test is somewhat polluted by the key generation but so is
        // the CPMap variant, therefore equally polluted. We create in default CP group as it's already created in the setup.
        // use uuid because we don't want collision as it will throw
        String key = UUID.randomUUID().toString();
        IAtomicReference<String> ar = targetInstance.getCPSubsystem().getAtomicReference(key);
        ar.set(v);
        ar.destroy();
    }

    public class ThreadState extends BaseThreadState {
        final IFunction<String, String> identity = s -> s;
    }
}