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
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * In this test we are using locks to control access to an IList of accounts.
 * We are using tryLock() with a configurable time out: tryLockTimeOutMs.
 * We verify that the total value of accounts is the same at the end of the test.
 */
public class TryLockTimeOutTest {

    private static final ILogger LOGGER = Logger.getLogger(TryLockTimeOutTest.class);

    public String basename = TryLockTimeOutTest.class.getSimpleName();
    public int threadCount = 3;
    public int maxAccounts = 100;
    public int tryLockTimeOutMs = 1;
    public long initialAccountValue = 1000;

    private long totalInitialValue;
    private TestContext testContext;
    private HazelcastInstance hazelcastInstance;

    @Setup
    public void setup(TestContext testContext) {
        this.testContext = testContext;
        hazelcastInstance = testContext.getTargetInstance();
    }

    @Warmup(global = true)
    public void warmup() {
        IList<Long> accounts = hazelcastInstance.getList(basename);
        for (int i = 0; i < maxAccounts; i++) {
            accounts.add(initialAccountValue);
        }
        totalInitialValue = initialAccountValue * maxAccounts;
        LOGGER.info("totalInitialValue=" + totalInitialValue);
    }

    @Verify(global = true)
    public void verify() {

        for (int i = 0; i < maxAccounts; i++) {
            ILock lock = hazelcastInstance.getLock(basename + i);
            assertFalse(basename + ": Lock should be unlocked", lock.isLocked());
        }

        long totalValue = 0;
        IList<Long> accounts = hazelcastInstance.getList(basename);
        for (long value : accounts) {
            totalValue += value;
        }
        LOGGER.info(": totalValue=" + totalValue);
        assertEquals(basename + ": totalInitialValue != totalValue ", totalInitialValue, totalValue);

        Counter total = new Counter();
        IList<Counter> totals = hazelcastInstance.getList(basename + "count");
        for (Counter count : totals) {
            total.add(count);
        }
        LOGGER.info("total count " + total);
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(basename);
        for (int i = 0; i < threadCount; i++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {

        private final Random random = new Random();
        private final Counter counter = new Counter();

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                int key1 = random.nextInt(maxAccounts);
                int key2 = random.nextInt(maxAccounts);

                ILock outerLock = hazelcastInstance.getLock(basename + key1);
                try {
                    if (outerLock.tryLock(tryLockTimeOutMs, TimeUnit.MILLISECONDS)) {
                        try {
                            innerLockOperation(key1, key2);
                        } finally {
                            outerLock.unlock();
                        }
                    }
                } catch (InterruptedException e) {
                    LOGGER.severe("outerLock " + e.getMessage(), e);
                    counter.interruptedException++;
                }
            }
            hazelcastInstance.getList(basename + "count").add(counter);
        }

        private void innerLockOperation(int key1, int key2) {
            ILock innerLock = hazelcastInstance.getLock(basename + key2);
            try {
                if (innerLock.tryLock(tryLockTimeOutMs, TimeUnit.MILLISECONDS)) {
                    try {
                        IList<Long> accounts = hazelcastInstance.getList(basename);
                        int delta = random.nextInt(100);

                        if (accounts.get(key1) >= delta) {
                            accounts.set(key1, accounts.get(key1) - delta);
                            accounts.set(key2, accounts.get(key2) + delta);
                            counter.transfers++;
                        }
                    } finally {
                        innerLock.unlock();
                    }
                }
            } catch (InterruptedException e) {
                LOGGER.severe("innerLock " + e.getMessage(), e);
                counter.interruptedException++;
            }
        }
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
}
