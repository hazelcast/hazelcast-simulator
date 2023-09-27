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
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.utils.GeneratorUtils;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;

/**
 * Writes [1,2,4,8,16]kb key(name) value atomic references. These sizes could be slightly off and assume (correctly, I think)
 * no compression whatsoever on our side of the keys-values. You shouldn't enable such compression to be safe even if possible.
 * <p>
 * Default is 1kb writes. You can specify the KB size by overriding the [keyValueSizeKb] configuration property.
 */
public class IAtomicReferenceTest extends HazelcastTest {
    public int keyValueSizeKb = 1;
    // [totalOps] is used as the most basic of assertions simplify to ensure we actually did something
    private AtomicLong totalOps;
    private IAtomicReference<String> atomicReference;

    @Setup
    public void setup() {
        totalOps = new AtomicLong();
        String kv = createString(keyValueSizeKb);
        atomicReference = targetInstance.getCPSubsystem().getAtomicReference(kv);
    }

    String createString(int kb) {
        int bytes = kb * 1024;
        int charsRequired = bytes / 2;
        return GeneratorUtils.generateAsciiString(charsRequired);
    }

    @TimeStep(prob = 1)
    public void set(ThreadState state) {
        atomicReference.set(atomicReference.getName());
        state.ops++;
    }

    @TimeStep(prob = 0)
    public void alter(ThreadState state) {
        atomicReference.alter(state.identity);
        state.ops++;
    }

    public class ThreadState extends BaseThreadState {
        long ops;

        final IFunction<String, String> identity = s -> s;
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        totalOps.addAndGet(state.ops);
    }

    @Verify
    public void verify() {
        assertTrue(totalOps.get() > 0);
    }
}
