/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ILock;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LockTest extends AbstractTest {

    // properties
    public int lockCount = 500;
    public int initialAmount = 1000;
    public int amount = 50;

    private IAtomicLong lockCounter;

    @Setup
    public void setup() {
        lockCounter = targetInstance.getAtomicLong(name + ":LockCounter");
    }

    @Prepare(global = true)
    public void prepare() {
        for (int i = 0; i < lockCount; i++) {
            long key = lockCounter.getAndIncrement();
            targetInstance.getLock(getLockId(key));
            IAtomicLong account = targetInstance.getAtomicLong(getAccountId(key));
            account.set(initialAmount);
        }
    }

    @TimeStep
    public void timeStep(ThreadState state) {
        long key1 = state.getRandomAccountKey();
        long key2 = state.getRandomAccountKey();
        int randomAmount = state.randomInt(amount);

        ILock lock1 = targetInstance.getLock(getLockId(key1));
        ILock lock2 = targetInstance.getLock(getLockId(key2));
        IAtomicLong account1 = targetInstance.getAtomicLong(getAccountId(key1));
        IAtomicLong account2 = targetInstance.getAtomicLong(getAccountId(key2));

        if (!lock1.tryLock()) {
            return;
        }
        try {
            if (!lock2.tryLock()) {
                return;
            }
            try {
                if (account1.get() < 0 || account2.get() < 0) {
                    throw new TestException("Amount on account can't be smaller than 0");
                }
                if (account1.get() < randomAmount) {
                    return;
                }
                account1.set(account1.get() - randomAmount);
                account2.set(account2.get() + randomAmount);
            } finally {
                lock2.unlock();
            }
        } finally {
            lock1.unlock();
        }
    }

    public class ThreadState extends BaseThreadState {

        private long getRandomAccountKey() {
            long key = randomLong() % lockCounter.get();
            return (key < 0) ? -key : key;
        }
    }

    private String getLockId(long key) {
        return name + '-' + key;
    }

    private String getAccountId(long key) {
        return name + '-' + key;
    }

    @Verify
    public void verify() {
        long actual = 0;
        for (long i = 0; i < lockCounter.get(); i++) {
            ILock lock = targetInstance.getLock(getLockId(i));
            assertFalse("Lock should be unlocked", lock.isLocked());

            long accountAmount = targetInstance.getAtomicLong(getAccountId(i)).get();
            assertTrue("Amount on account can't be smaller than 0", accountAmount >= 0);
            actual += accountAmount;
        }

        long expected = initialAmount * lockCounter.get();
        assertEquals(format("%s: Money was lost or created (%d)", name, expected - actual), expected, actual);
    }

    @Teardown
    public void teardown() {
        lockCounter.destroy();

        for (long i = 0; i < lockCounter.get(); i++) {
            targetInstance.getLock(getLockId(i)).destroy();
            targetInstance.getAtomicLong(getAccountId(i)).destroy();
        }
    }
}
