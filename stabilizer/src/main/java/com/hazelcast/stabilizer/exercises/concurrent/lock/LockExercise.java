package com.hazelcast.stabilizer.exercises.concurrent.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ILock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.exercises.AbstractExercise;
import com.hazelcast.stabilizer.exercises.ExerciseRunner;

import java.util.Random;

public class LockExercise extends AbstractExercise {

    private final static ILogger log = Logger.getLogger(LockExercise.class);

    public int lockCount = 500;
    public int threadCount = 10;
    public int initialAmount = 1000;
    public int amount = 50;
    public int logFrequency = 1000;

    private IAtomicLong lockCounter;
    private IAtomicLong totalMoney;

    @Override
    public void localSetup() throws Exception {
        super.localSetup();

        HazelcastInstance targetInstance = getTargetInstance();

        lockCounter = targetInstance.getAtomicLong(getExerciseId() + ":LockCounter");
        totalMoney = targetInstance.getAtomicLong(getExerciseId() + ":TotalMoney");

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
        return getExerciseId() + ":Lock" + key;
    }

    private String getAccountId(long key) {
        return getExerciseId() + ":Account" + key;
    }

    @Override
    public void globalVerify() {
        long foundTotal = 0;
        for (long k = 0; k < lockCounter.get(); k++) {
            ILock lock = hazelcastInstance.getLock(getLockId(k));
            if (lock.isLocked()) {
                throw new RuntimeException("Lock should be unlocked");
            }

            IAtomicLong account = hazelcastInstance.getAtomicLong(getAccountId(k));
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
            hazelcastInstance.getLock(getLockId(k)).destroy();
            hazelcastInstance.getAtomicLong(getAccountId(k)).destroy();
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

                IAtomicLong account1 = hazelcastInstance.getAtomicLong(getAccountId(key1));
                ILock lock1 = hazelcastInstance.getLock(getLockId(key1));
                IAtomicLong account2 = hazelcastInstance.getAtomicLong(getAccountId(key2));
                ILock lock2 = hazelcastInstance.getLock(getLockId(key2));

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
        LockExercise mapExercise = new LockExercise();
        new ExerciseRunner().run(mapExercise, 20);
    }
}


