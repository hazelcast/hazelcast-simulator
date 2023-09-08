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

import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;

/**
 * Shared counter ({@link IAtomicLong}) that is incremented/read by the configured number of clients/threads.
 */
public class IAtomicLongSharedCounterTest extends HazelcastTest {
    private IAtomicLong sharedCounter;
    private AtomicLong totalWorkerIncrements;

    @Setup
    public void setup() {
        sharedCounter = targetInstance.getCPSubsystem().getAtomicLong("atomiclong-" + name);
        totalWorkerIncrements = new AtomicLong();
    }

    @TimeStep(prob = 0)
    public long get(ThreadState state) {
        return sharedCounter.get();
    }

    @TimeStep(prob = 1)
    public long increment(ThreadState state) {
        long result = sharedCounter.incrementAndGet();
        state.increments++;
        return result;
    }

    public class ThreadState extends BaseThreadState {
        long increments;
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        totalWorkerIncrements.addAndGet(state.increments);
    }

    @Verify
    public void verify() {
        assertTrue(sharedCounter.get() >= totalWorkerIncrements.get());
    }

    @Teardown
    public void teardown() {
        // destruction won't help us here...
    }
}
