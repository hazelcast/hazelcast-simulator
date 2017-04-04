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

import com.hazelcast.core.ILock;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.fail;

public class LeaseLockTest extends HazelcastTest {

    public int lockCount = 500;
    public int maxLeaseTimeMillis = 100;
    public int maxTryTimeMillis = 100;
    public boolean allowZeroMillisRemainingLeaseLockTime = false;

    @TimeStep
    public void timeStep(BaseThreadState state) {
        int lockIndex = state.randomInt(lockCount);
        ILock lock = targetInstance.getLock(name + lockIndex);

        int leaseTime = 1 + state.randomInt(maxLeaseTimeMillis);
        int tryTime = 1 + state.randomInt(maxTryTimeMillis);

        if (state.randomBoolean()) {
            lock.lock(leaseTime, MILLISECONDS);
        } else {
            try {
                lock.tryLock(tryTime, MILLISECONDS, leaseTime, MILLISECONDS);
            } catch (InterruptedException e) {
                logger.info("tryLock() got exception: " + e.getMessage());
            }
        }
    }

    @AfterRun
    public void afterRun() throws Exception {
        sleepMillis((maxTryTimeMillis + maxLeaseTimeMillis) * 2);
    }

    @Verify
    public void verify() {
        for (int i = 0; i < lockCount; i++) {
            ILock lock = targetInstance.getLock(name + i);

            boolean isLocked = lock.isLocked();
            long remainingLeaseTime = lock.getRemainingLeaseTime();
            if (isLocked) {
                String message = format("%s is locked with remainingLeaseTime: %d ms", lock, remainingLeaseTime);
                if (allowZeroMillisRemainingLeaseLockTime && remainingLeaseTime == 0) {
                    logger.warn(message);
                } else {
                    fail(message);
                }
            }
            if (remainingLeaseTime > 0) {
                fail(format("%s has remainingLeaseTime: %d ms", lock, remainingLeaseTime));
            }
        }
    }
}
