package com.hazelcast.stabilizer.tests.concurrent.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ILock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.*;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.Random;

import static org.junit.Assert.*;

public class SimpleLockTest {

    private final static ILogger log = Logger.getLogger(SimpleLockTest.class);

    public int maxAccounts = 7;
    public int threadCount = 10;
    public int logFrequency = 10000;

    private String basename = this.getClass().getName();
    private int initialValue=1000;
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
            IAtomicLong account = targetInstance.getAtomicLong(basename+k);
            account.set(initialValue);
        }
        totalValue = initialValue * maxAccounts;
    }

    @Verify
    public void verify() {

        int value = 0;
        for (int k = 0; k < maxAccounts; k++) {
            ILock lock = targetInstance.getLock(basename+k);
            IAtomicLong account = targetInstance.getAtomicLong(basename+k);

            System.out.println(account+" "+account.get());

            assertFalse("Lock should be unlocked", lock.isLocked());
            assertTrue("Amount is !< 0 ", account.get() >= 0);
            value += account.get();
        }
        assertEquals(totalValue, value);
    }

    @Teardown
    public void teardown() throws Exception { }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            int key1;
            int key2;
            while (!testContext.isStopped()) {

                key1 = random.nextInt(maxAccounts);
                do{
                    key2 = random.nextInt(maxAccounts);
                }while(key1 == key2);


                ILock lock1 = targetInstance.getLock(basename+key1);
                if (lock1.tryLock()) {
                    try {
                        ILock lock2 = targetInstance.getLock(basename+key2);
                        if (lock2.tryLock()) {
                            try {
                                IAtomicLong account1 = targetInstance.getAtomicLong(basename+key1);
                                IAtomicLong account2 = targetInstance.getAtomicLong(basename+key2);

                                int delta = random.nextInt(100);
                                if(account1.get() >= delta){
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

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                iteration++;
            }
        }
    }


    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }


    public static void main(String[] args) throws Throwable {
        SimpleLockTest test = new SimpleLockTest();
        new TestRunner(test).run();
    }
}


