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

import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * In this test we are using locks to control access to an IList of accounts.
 * We are using tryLock() with a configurable time out: tryLockTimeOutMs.
 * We verify that the total value of accounts is the same at the end of the test.
 */
public class TryLockTimeOutTest extends HazelcastTest {

    public int threadCount = 3;
    public int maxAccounts = 100;
    public int tryLockTimeOutMs = 1;
    public long initialAccountValue = 1000;

    private long totalInitialValue;

    @Prepare(global = true)
    public void prepare() {
        IList<Long> accounts = targetInstance.getList(name);
        for (int i = 0; i < maxAccounts; i++) {
            accounts.add(initialAccountValue);
        }
        totalInitialValue = initialAccountValue * maxAccounts;
        logger.info("totalInitialValue=" + totalInitialValue);
    }

    @SuppressWarnings("checkstyle:nestedtrydepth")
    @TimeStep
    public void timeStep(ThreadState state) {
        int key1 = state.randomInt(maxAccounts);
        int key2 = state.randomInt(maxAccounts);

        ILock outerLock = targetInstance.getLock(name + key1);
        try {
            if (outerLock.tryLock(tryLockTimeOutMs, TimeUnit.MILLISECONDS)) {
                try {
                    ILock innerLock = targetInstance.getLock(name + key2);
                    try {
                        if (innerLock.tryLock(tryLockTimeOutMs, TimeUnit.MILLISECONDS)) {
                            try {
                                IList<Long> accounts = targetInstance.getList(name);
                                int delta = state.random.nextInt(100);

                                if (accounts.get(key1) >= delta) {
                                    accounts.set(key1, accounts.get(key1) - delta);
                                    accounts.set(key2, accounts.get(key2) + delta);
                                    state.counter.transfers++;
                                }
                            } finally {
                                innerLock.unlock();
                            }
                        }
                    } catch (InterruptedException e) {
                        logger.fatal("innerLock " + e.getMessage(), e);
                        state.counter.interruptedException++;
                    }
                } finally {
                    outerLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            logger.fatal("outerLock " + e.getMessage(), e);
            state.counter.interruptedException++;
        }
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        targetInstance.getList(name + "count").add(state.counter);
    }

    public class ThreadState extends BaseThreadState {
        private final Counter counter = new Counter();
    }

    private static class Counter implements Serializable {

        public long interruptedException = 0;
        public long transfers = 0;

        public void add(Counter c) {
            interruptedException += c.interruptedException;
            transfers += c.transfers;
        }

        @Override
        public String toString() {
            return "Counter{"
                    + "interruptedException=" + interruptedException
                    + ", transfers=" + transfers
                    + '}';
        }
    }

    @Verify(global = true)
    public void verify() {
        for (int i = 0; i < maxAccounts; i++) {
            ILock lock = targetInstance.getLock(name + i);
            assertFalse(name + ": Lock should be unlocked", lock.isLocked());
        }

        long totalValue = 0;
        IList<Long> accounts = targetInstance.getList(name);
        for (long value : accounts) {
            totalValue += value;
        }
        logger.info(": totalValue=" + totalValue);
        assertEquals(name + ": totalInitialValue != totalValue ", totalInitialValue, totalValue);

        Counter total = new Counter();
        IList<Counter> totals = targetInstance.getList(name + "count");
        for (Counter count : totals) {
            total.add(count);
        }
        logger.info("total count " + total);
    }
}
