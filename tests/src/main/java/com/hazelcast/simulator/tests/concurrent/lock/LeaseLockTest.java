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
package com.hazelcast.simulator.tests.concurrent.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static java.lang.String.format;
import static org.junit.Assert.fail;

public class LeaseLockTest {

    private static final ILogger LOGGER = Logger.getLogger(LeaseLockTest.class);

    public String basename = LeaseLockTest.class.getSimpleName();
    public int lockCount = 500;
    public int maxLeaseTimeMillis = 100;
    public int maxTryTimeMillis = 100;

    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) {
        targetInstance = testContext.getTargetInstance();
    }

    @Verify
    public void verify() {
        sleepMillis((maxTryTimeMillis + maxLeaseTimeMillis) * 2);

        for (int i = 0; i < lockCount; i++) {
            ILock lock = targetInstance.getLock(basename + i);

            boolean isLocked = lock.isLocked();
            long remainingLeaseTime = lock.getRemainingLeaseTime();
            if (isLocked) {
                fail(format("%s is locked with remainingLeaseTime: %d ms", lock, remainingLeaseTime));
            }
            if (remainingLeaseTime > 0) {
                fail(format("%s has remainingLeaseTime: %d ms", lock, remainingLeaseTime));
            }
        }
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        public void timeStep() {
            int lockIndex = randomInt(lockCount);
            ILock lock = targetInstance.getLock(basename + lockIndex);

            int leaseTime = 1 + randomInt(maxLeaseTimeMillis);
            int tryTime = 1 + randomInt(maxTryTimeMillis);

            if (getRandom().nextBoolean()) {
                lock.lock(leaseTime, TimeUnit.MILLISECONDS);
            } else {
                try {
                    lock.tryLock(tryTime, TimeUnit.MILLISECONDS, leaseTime, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOGGER.info("tryLock() got exception: " + e.getMessage());
                }
            }
        }
    }
}
