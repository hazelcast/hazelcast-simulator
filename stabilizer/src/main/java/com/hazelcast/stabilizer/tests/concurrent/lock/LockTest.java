package com.hazelcast.stabilizer.tests.concurrent.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ILock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.AbstractTest;
import com.hazelcast.stabilizer.tests.TestRunner;

import java.util.Random;

public class LockTest extends AbstractTest {

    private final static ILogger log = Logger.getLogger(LockTest.class);

    public int lockCount = 500;
    public int threadCount = 10;
    public int initialAmount = 1000;
    public int amount = 50;
    public int logFrequency = 1000;

    private IAtomicLong lockCounter;
    private IAtomicLong totalMoney;

    @Override
    public void localSetup() throws Exception {
        HazelcastInstance targetInstance = getTargetInstance();

        lockCounter = targetInstance.getAtomicLong(getTestId() + ":LockCounter");
        totalMoney = targetInstance.getAtomicLong(getTestId() + ":TotalMoney");

        for (int k = 0; k < lockCount; k++) {
            long key = lockCounter.getAndIncrement();
            targetInstance.getLock(getLockId(key));
            IAtomicLong account = targetInstance.getAtomicLong(getAccountId(key));
            account.set(initialAmount);
            totalMoney.addAndGet(initialAmount);
        }

        for (int k = 0; k < threadCount; k++) {
            spawn(new Worker());
        }
    }

    private String getLockId(long key) {
        return getTestId() + ":Lock" + key;
    }

    private String getAccountId(long key) {
        return getTestId() + ":Account" + key;
    }

    @Override
    public void globalVerify() {
        long foundTotal = 0;
        for (long k = 0; k < lockCounter.get(); k++) {
            ILock lock = serverInstance.getLock(getLockId(k));
            if (lock.isLocked()) {
                throw new RuntimeException("Lock should be unlocked");
            }

            IAtomicLong account = serverInstance.getAtomicLong(getAccountId(k));
            if (account.get() < 0) {
                throw new RuntimeException("Amount can't be smaller than zero on account");
            }
            System.out.println("acount:" + account.get());

            foundTotal += account.get();
        }

        if (foundTotal != totalMoney.get()) {
            throw new RuntimeException("Money was lost/created: Found money was: "
                    + foundTotal + " expected:" + totalMoney.get());
        }
    }

    @Override
    public void globalTearDown() throws Exception {
        lockCounter.destroy();
        totalMoney.destroy();

        for (long k = 0; k < lockCounter.get(); k++) {
            serverInstance.getLock(getLockId(k)).destroy();
            serverInstance.getAtomicLong(getAccountId(k)).destroy();
        }
    }

    private class Worker implements Runnable {
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            while (!stop) {
                long key1 = getRandomAccountKey();
                long key2 = getRandomAccountKey();
                int a = random.nextInt(amount);

                IAtomicLong account1 = serverInstance.getAtomicLong(getAccountId(key1));
                ILock lock1 = serverInstance.getLock(getLockId(key1));
                IAtomicLong account2 = serverInstance.getAtomicLong(getAccountId(key2));
                ILock lock2 = serverInstance.getLock(getLockId(key2));

                if (!lock1.tryLock()) {
                    continue;
                }

                try {
                    if (!lock2.tryLock()) {
                        continue;
                    }

                    try {
                        if (account1.get() < 0 || account2.get() < 0) {
                            throw new RuntimeException("Amount on account can't be smaller than 0");
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

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                iteration++;
            }

//            totalCounter.addAndGet(iteration);
        }

        private long getRandomAccountKey() {
            long key = random.nextLong() % lockCounter.get();


            if (key < 0) key = -key;
            return key;
        }
    }

    public static void main(String[] args) throws Exception {
        LockTest test = new LockTest();
        new TestRunner().run(test, 20);
    }
}


