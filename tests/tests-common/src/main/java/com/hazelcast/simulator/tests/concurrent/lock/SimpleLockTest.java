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

import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ILock;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.util.Random;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleLockTest extends AbstractTest {

    private static final int INITIAL_VALUE = 1000;

    public int maxAccounts = 7;
    public int threadCount = 10;

    private int totalValue = 0;

    @Warmup(global = true)
    public void warmup() {
        for (int i = 0; i < maxAccounts; i++) {
            IAtomicLong account = targetInstance.getAtomicLong(name + i);
            account.set(INITIAL_VALUE);
        }
        totalValue = INITIAL_VALUE * maxAccounts;
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(name);
        for (int i = 0; i < threadCount; i++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            int key1;
            int key2;
            while (!testContext.isStopped()) {

                key1 = random.nextInt(maxAccounts);
                do {
                    key2 = random.nextInt(maxAccounts);
                } while (key1 == key2);

                ILock lock1 = targetInstance.getLock(name + key1);
                if (lock1.tryLock()) {
                    try {
                        ILock lock2 = targetInstance.getLock(name + key2);
                        if (lock2.tryLock()) {
                            try {
                                IAtomicLong account1 = targetInstance.getAtomicLong(name + key1);
                                IAtomicLong account2 = targetInstance.getAtomicLong(name + key2);

                                int delta = random.nextInt(100);
                                if (account1.get() >= delta) {
                                    account1.set(account1.get() - delta);
                                    account2.set(account2.get() + delta);
                                }
                            } finally {
                                lock2.unlock();
                            }
                        }
                    } finally {
                        lock1.unlock();
                    }
                }
            }
        }
    }

    @Verify
    public void verify() {
        int value = 0;
        for (int i = 0; i < maxAccounts; i++) {
            ILock lock = targetInstance.getLock(name + i);
            IAtomicLong account = targetInstance.getAtomicLong(name + i);

            logger.info(format("%s %d", account, account.get()));

            assertFalse(name + ": Lock should be unlocked", lock.isLocked());
            assertTrue(name + ": Amount is < 0 ", account.get() >= 0);
            value += account.get();
        }
        assertEquals(name + " totals not adding up ", totalValue, value);
    }

}
