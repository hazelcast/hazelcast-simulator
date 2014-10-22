package com.hazelcast.stabilizer.tests.concurrent.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/*
* in this test we are using lock to controle the access to an Ilist of accounts
* we are using tryLock with a configurable time out: tryLockTimeOutMs
* we verify that the total value of accounts is the same at the end of the test
* */
public class TryLockTimeOutTest {

    private final static ILogger log = Logger.getLogger(TryLockTimeOutTest.class);

    public int threadCount = 3;
    public int maxAccounts = 100;
    public int tryLockTimeOutMs = 1;
    public long initialAccountValue = 1000;

    private long totalInitalValue;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private String basename;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        basename = testContext.getTestId();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        IList<Long> accounts = targetInstance.getList(basename);
        for (int k = 0; k < maxAccounts; k++) {
            accounts.add(initialAccountValue);
        }
        totalInitalValue = initialAccountValue * maxAccounts;
        log.info(basename + ": totalInitalValue=" + totalInitalValue);
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        private final Random random = new Random();
        private long InterruptedExceptionCount=0;

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                int key1 = random.nextInt(maxAccounts);
                int key2 = random.nextInt(maxAccounts);

                ILock lock1 = targetInstance.getLock(basename + key1);
                try{
                    if (lock1.tryLock(tryLockTimeOutMs, TimeUnit.MILLISECONDS)) {
                        try {
                            ILock lock2 = targetInstance.getLock(basename + key2);

                            try {
                                if (lock2.tryLock(tryLockTimeOutMs, TimeUnit.MILLISECONDS) ) {
                                    try {
                                        IList<Long> accounts = targetInstance.getList(basename);
                                        int delta = random.nextInt(100);

                                        if (accounts.get(key1) >= delta) {
                                            accounts.set(key1, accounts.get(key1) - delta );
                                            accounts.set(key2, accounts.get(key2) + delta );
                                        }

                                    } finally {
                                        lock2.unlock();
                                    }
                                }
                            }catch(InterruptedException e){
                                log.severe(basename+": lock2 "+e, e);
                                InterruptedExceptionCount++;
                            }
                        } finally {
                            lock1.unlock();
                        }
                    }
                }catch(InterruptedException e){
                    log.severe(basename+": lock1 "+e, e);
                    InterruptedExceptionCount++;
                }
            }
            targetInstance.getList(basename+"exceptions").add(InterruptedExceptionCount);
        }
    }

    @Verify(global = true)
    public void verify() {

        for (int k = 0; k < maxAccounts; k++) {
            ILock lock = targetInstance.getLock(basename + k);
            assertFalse(basename + ": Lock should be unlocked", lock.isLocked());
        }

        long totalValue = 0;
        IList<Long> accounts = targetInstance.getList(basename);
        for (long value : accounts) {
            totalValue += value;
        }
        log.info(basename + ": totalValue=" + totalValue);
        assertEquals(basename+" totalInitalValue != totalValue ", totalInitalValue, totalValue);

        long totalExceptions=0;
        IList<Long> exceptionCount = targetInstance.getList(basename+"exceptions");
        for (long count : exceptionCount) {
            totalExceptions += count;
        }
        log.info(basename + ": total exceptions " + totalExceptions);
    }
}


