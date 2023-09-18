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

import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;

/**
 * Writes [1,2,4,8,16]kb key(name) value atomic references. These sizes could be slightly off and assume (correctly, I think)
 * no compression whatsoever on our side of the keys-values. You shouldn't enable such compression to be safe even if possible.
 * <p>
 * Default is 1kb writes.
 */
public class IAtomicReferenceTest extends HazelcastTest {
    private AtomicLong totalWrites;

    private List<IAtomicReference<String>> atomicReferences;

    @Setup
    public void setup() {
        totalWrites = new AtomicLong();
        atomicReferences = new ArrayList<>();
        for (int i = 1; i <= 16; i *= 2) {
            String kv = createString(i);
            atomicReferences.add(targetInstance.getCPSubsystem().getAtomicReference(kv));
        }
    }

    String createString(int kb) {
        int bytes = kb * 1024;
        int charsRequired = bytes / 2;
        StringBuilder sb = new StringBuilder(bytes);
        for (int i = 0; i < charsRequired; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

    @TimeStep(prob = 1)
    public void oneKbWrite(ThreadState state) {
        write(0, state);
    }

    @TimeStep(prob = 0)
    public void twoKbWrite(ThreadState state) {
        write(1, state);
    }

    @TimeStep(prob = 0)
    public void fourKbWrite(ThreadState state) {
        write(2, state);
    }

    @TimeStep(prob = 0)
    public void eightKbWrite(ThreadState state) {
        write(3, state);
    }

    @TimeStep(prob = 0)
    public void sixteenKbWrite(ThreadState state) {
        write(4, state);
    }

    private void write(int index, ThreadState state) {
        IAtomicReference<String> atomicReference = atomicReferences.get(index);
        atomicReference.set(atomicReference.getName());
        state.writes++;
    }

    public class ThreadState extends BaseThreadState {
        long writes;
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        totalWrites.addAndGet(state.writes);
    }

    @Verify
    public void verify() {
        assertTrue(totalWrites.get() > 0);
    }
}
