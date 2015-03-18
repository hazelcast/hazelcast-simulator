package com.hazelcast.simulator.tests.concurrent.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ILock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.test.utils.ThreadSpawner;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleLockTest {

    private static final ILogger log = Logger.getLogger(SimpleLockTest.class);

    public String basename = this.getClass().getSimpleName();
    public int maxAccounts = 7;
    public int threadCount = 10;

    private int initialValue = 1000;
    private int totalValue = 0;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        for (int k = 0; k < maxAccounts; k++) {
            IAtomicLong account = targetInstance.getAtomicLong(basename + k);
            account.set(initialValue);
        }
        totalValue = initialValue * maxAccounts;
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

                ILock lock1 = targetInstance.getLock(basename + key1);
                if (lock1.tryLock()) {
                    try {
                        ILock lock2 = targetInstance.getLock(basename + key2);
                        if (lock2.tryLock()) {
                            try {
                                IAtomicLong account1 = targetInstance.getAtomicLong(basename + key1);
                                IAtomicLong account2 = targetInstance.getAtomicLong(basename + key2);

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
        for (int k = 0; k < maxAccounts; k++) {
            ILock lock = targetInstance.getLock(basename + k);
            IAtomicLong account = targetInstance.getAtomicLong(basename + k);

            log.info(account + " " + account.get());

            assertFalse(basename + ": Lock should be unlocked", lock.isLocked());
            assertTrue(basename + ": Amount is < 0 ", account.get() >= 0);
            value += account.get();
        }
        assertEquals(basename + " totals not adding up ", totalValue, value);
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }
}


