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
import com.hazelcast.simulator.test.utils.ThreadSpawner;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/*
* in this test we are using lock to control access to an Ilist of accounts
* we are using tryLock with a configurable time out: tryLockTimeOutMs
* we verify that the total value of accounts is the same at the end of the test
* */
public class TryLockTimeOutTest {

    private ILogger log;

    public int threadCount = 3;
    public int maxAccounts = 100;
    public int tryLockTimeOutMs = 1;
    public long initialAccountValue = 1000;
    public String basename = this.getClass().getSimpleName();

    private long totalInitalValue;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private String id;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        id = testContext.getTestId();
        log = Logger.getLogger(TryLockTimeOutTest.class+" "+id);
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        IList<Long> accounts = targetInstance.getList(basename);
        for (int k = 0; k < maxAccounts; k++) {
            accounts.add(initialAccountValue);
        }
        totalInitalValue = initialAccountValue * maxAccounts;
        log.info(" totalInitalValue=" + totalInitalValue);
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
        private Counter counter = new Counter();

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
                                            counter.transfers++;
                                        }

                                    } finally {
                                        lock2.unlock();
                                    }
                                }
                            }catch(InterruptedException e){
                                log.severe(" lock2 "+e, e);
                                counter.interruptedException++;
                            }
                        } finally {
                            lock1.unlock();
                        }
                    }
                }catch(InterruptedException e){
                    log.severe(" lock1 "+e, e);
                    counter.interruptedException++;
                }
            }
            targetInstance.getList(basename+"count").add(counter);
        }
    }

    private static class Counter implements Serializable {
        public long interruptedException=0;
        public long transfers=0;

        public void add(Counter c) {
            interruptedException += c.interruptedException;
            transfers += c.transfers;
        }

        @Override
        public String toString() {
            return "Counter{" +
                    "interruptedException=" + interruptedException +
                    ", transfers=" + transfers +
                    '}';
        }
    }

    @Verify(global = true)
    public void verify() {

        for (int k = 0; k < maxAccounts; k++) {
            ILock lock = targetInstance.getLock(basename + k);
            assertFalse(id + " Lock should be unlocked", lock.isLocked());
        }

        long totalValue = 0;
        IList<Long> accounts = targetInstance.getList(basename);
        for (long value : accounts) {
            totalValue += value;
        }
        log.info(": totalValue=" + totalValue);
        assertEquals(id + " totalInitalValue != totalValue ", totalInitalValue, totalValue);

        Counter total = new Counter();
        IList<Counter> totals = targetInstance.getList(basename+"count");
        for (Counter count : totals) {
            total.add(count);
        }
        log.info(" total count " + total);
    }
}