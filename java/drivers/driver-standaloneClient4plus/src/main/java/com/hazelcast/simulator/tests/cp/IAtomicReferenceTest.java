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
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateAsciiString;

public class IAtomicReferenceTest extends HazelcastTest {
    //the size of the value in bytes.
    public int valueSize = 1;
    // the number of IAtomicReferences
    public int referenceCount = 1;
    // the number of CPGroups. 0 means that the default CPGroup is used.
    // The AtomicRefs will be placed over the different CPGroups in round robin fashion.
    public int cpGroupCount = 0;

    private IAtomicReference<String>[] references;
    private String value; // this is always the value of [atomicReference]; before + after, irrespective of the op

    @Setup
    public void setup() {
        value = generateAsciiString(valueSize);

        CPSubsystem cpSubsystem = targetInstance.getCPSubsystem();

        references = new IAtomicReference[referenceCount];
        for (int k = 0; k < referenceCount; k++) {
            String cpGroupString = cpGroupCount == 0
                    ? ""
                    : "@" + (k % cpGroupCount);
            references[k] = cpSubsystem.getAtomicReference("ref-"+k + cpGroupString);
        }
    }

    @Prepare(global = true)
    public void prepare() {
        for (IAtomicReference<String> reference : references) {
            reference.set(value);
        }
    }

    @TimeStep(prob = 0)
    public String get(ThreadState state) {
        IAtomicReference<String> reference = state.getNextAtomicReference();
        return reference.get();
    }

    @TimeStep(prob = 1)
    public void set(ThreadState state) {
        IAtomicReference<String> reference = state.getNextAtomicReference();
        reference.set(value);
    }

    @TimeStep(prob = 0)
    public void alter(ThreadState state) {
        IAtomicReference<String> reference = state.getNextAtomicReference();
        reference.alter(state.identity);
    }

    @TimeStep(prob = 0)
    public boolean cas(ThreadState state) {
        IAtomicReference<String> reference = state.getNextAtomicReference();
        return reference.compareAndSet(value, value);
    }

    @TimeStep(prob = 0)
    public void casOptimisticConcurrencyControl(ThreadState state) {
        IAtomicReference<String> reference = state.getNextAtomicReference();

        // todo: what is the point of the loop? It will always succeed because there is just a single value.
        // So the performance will be exactly the same as the cas timestep method.
        String observed;
        String newValue;
        do {
            observed = reference.get(); // because we're modelling the pattern -- we know it's [v]...
            newValue = observed;
        } while (!reference.compareAndSet(observed, newValue));
    }

    public class ThreadState extends BaseThreadState {

        private int currentReferenceIndex = 0;

        public IAtomicReference<String> getNextAtomicReference() {
            if (currentReferenceIndex == referenceCount) {
                currentReferenceIndex = 0;
            }
            return references[currentReferenceIndex++];
        }

        final IFunction<String, String> identity = s -> s;
    }
}
