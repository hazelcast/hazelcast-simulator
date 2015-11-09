/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ILock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LockTest {

    private static final ILogger LOGGER = Logger.getLogger(LockTest.class);

    // properties
    public String basename = LockTest.class.getSimpleName();
    public int lockCount = 500;
    public int threadCount = 10;
    public int initialAmount = 1000;
    public int amount = 50;
    public int logFrequency = 1000;

    private IAtomicLong lockCounter;
    private IAtomicLong totalMoney;
    private HazelcastInstance targetInstance;
    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

        lockCounter = targetInstance.getAtomicLong(basename + ":LockCounter");
        totalMoney = targetInstance.getAtomicLong(basename + ":TotalMoney");
    }

    @Warmup(global = true)
    public void warmup() {
        for (int i = 0; i < lockCount; i++) {
            long key = lockCounter.getAndIncrement();
            targetInstance.getLock(getLockId(key));
            IAtomicLong account = targetInstance.getAtomicLong(getAccountId(key));
            account.set(initialAmount);
            totalMoney.addAndGet(initialAmount);
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(basename);
        for (int i = 0; i < threadCount; i++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private String getLockId(long key) {
        return basename + '-' + key;
    }

    private String getAccountId(long key) {
        return basename + '-' + key;
    }

    @Verify
    public void verify() {
        long actual = 0;
        for (long k = 0; k < lockCounter.get(); k++) {
            ILock lock = targetInstance.getLock(getLockId(k));
            assertFalse("Lock should be unlocked", lock.isLocked());

            IAtomicLong account = targetInstance.getAtomicLong(getAccountId(k));
            assertTrue("Amount can't be smaller than zero on account", account.get() >= 0);
            actual += account.get();
        }

        long expected = totalMoney.get();
        assertEquals(basename + ": Money was lost/created", expected, actual);
    }

    @Teardown
    public void teardown() {
        lockCounter.destroy();
        totalMoney.destroy();

        for (long k = 0; k < lockCounter.get(); k++) {
            targetInstance.getLock(getLockId(k)).destroy();
            targetInstance.getAtomicLong(getAccountId(k)).destroy();
        }
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            while (!testContext.isStopped()) {
                long key1 = getRandomAccountKey();
                long key2 = getRandomAccountKey();
                int a = random.nextInt(amount);

                IAtomicLong account1 = targetInstance.getAtomicLong(getAccountId(key1));
                ILock lock1 = targetInstance.getLock(getLockId(key1));
                IAtomicLong account2 = targetInstance.getAtomicLong(getAccountId(key2));
                ILock lock2 = targetInstance.getLock(getLockId(key2));

                if (!lock1.tryLock()) {
                    continue;
                }

                try {
                    if (!lock2.tryLock()) {
                        continue;
                    }

                    try {
                        if (account1.get() < 0 || account2.get() < 0) {
                            throw new TestException("Amount on account can't be smaller than 0");
                        }

                        if (account1.get() < a) {
                            continue;
                        }

                        account1.set(account1.get() - a);
                        account2.set(account2.get() + a);
                    } finally {
                        lock2.unlock();
                    }

                } finally {
                    lock1.unlock();
                }

                iteration++;
                if (iteration % logFrequency == 0) {
                    LOGGER.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }
            }
            totalMoney.addAndGet(iteration);
        }

        private long getRandomAccountKey() {
            long key = random.nextLong() % lockCounter.get();

            if (key < 0) {
                key = -key;
            }
            return key;
        }
    }

    public static void main(String[] args) throws Exception {
        LockTest test = new LockTest();
        new TestRunner<LockTest>(test).withDuration(10).run();
    }
}
