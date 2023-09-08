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

import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;

/**
 * Acq/release for uncontended and contented locks. Only a single thread per-client should be used. Multiple clients are fine.
 * This is because the uncontended locks are locks that I want to have no contention on.
 */
public class FencedLockTest extends HazelcastTest {
    private static final List<String> CONTENDED_LOCKS =
            List.of("mickey", "donald", "minnie", "daffy", "woody", "buzz", "stitch", "elsa", "ariel", "mulan");
    private AtomicLong totalWorkerAcquireReleases;

    private FencedLock[] contendedLocks;
    private FencedLock[] uncontendedLocks; // these are locks I'm only accessing, I'm == my client; remember one thread please

    @Setup
    public void setup() {
        totalWorkerAcquireReleases = new AtomicLong();
        contendedLocks = new FencedLock[CONTENDED_LOCKS.size()];
        for (int i = 0 ; i < contendedLocks.length; i++) {
            contendedLocks[i] = targetInstance.getCPSubsystem().getLock(CONTENDED_LOCKS.get(i));
        }

        uncontendedLocks = new FencedLock[1_000];
        for (int i = 0 ; i < uncontendedLocks.length; i++) {
            uncontendedLocks[i] = targetInstance.getCPSubsystem().getLock(UUID.randomUUID().toString());
        }
    }

    @TimeStep(prob = 0)
    public void acquireReleaseUncontended(ThreadState state) {
        FencedLock lock = state.randomUncontendedLock();
        lock.lock();
        try {
        } finally {
            lock.unlock();
        }
        state.acquireReleases++;
    }

    @TimeStep(prob = 0)
    public void acquireReleaseContended(ThreadState state) {
        FencedLock lock = state.randomContendedLock();
        lock.lock();
        try {
        } finally {
            lock.unlock();
        }
        state.acquireReleases++;
    }

    public class ThreadState extends BaseThreadState {
        long acquireReleases;
        private FencedLock randomContendedLock() {
            int index = randomInt(contendedLocks.length);
            return contendedLocks[index];
        }

        private FencedLock randomUncontendedLock() {
            int index = randomInt(uncontendedLocks.length);
            return uncontendedLocks[index];
        }
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        totalWorkerAcquireReleases.addAndGet(state.acquireReleases);
    }

    @Verify
    public void verify() {
        assertTrue(totalWorkerAcquireReleases.get() > 0);
    }

    @Teardown
    public void teardown() {
        // destruction won't help us here...
    }
}
