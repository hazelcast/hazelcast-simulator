package com.hazelcast.stabilizer.tests.concurrent.lock;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.tests.helpers.KeyIncrementPair;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.stabilizer.utils.CommonUtils.sleepSeconds;
import static org.junit.Assert.assertEquals;

// TODO: We need to deal with exception logging; they are logged but not visible to stabilizer.
public class LockConflictTest {

    private final static ILogger log = Logger.getLogger(LockConflictTest.class);

    // properties.
    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int keyCount = 50;
    public int maxKeysPerTxn = 5;
    public int tryLockTimeOutMs = 10;
    public boolean throwException=false;

    private HazelcastInstance targetInstance;
    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        IList<Long> accounts = targetInstance.getList(basename);
        for (int k = 0; k < keyCount; k++) {
            accounts.add(0l);
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

    private class Worker implements Runnable {
        private final Random random = new Random();
        private final long[] localIncrements = new long[keyCount];

        private final LockCounter counter = new LockCounter();

        @Override
        public void run() {
            while (!testContext.isStopped()) {

                List<KeyIncrementPair> potentialLocks = new ArrayList();
                for (int i = 0; i < maxKeysPerTxn; i++) {
                    KeyIncrementPair p = new KeyIncrementPair(random, keyCount, 999);
                    potentialLocks.add(p);
                }

                List<KeyIncrementPair> locked = new ArrayList();
                for (KeyIncrementPair i : potentialLocks) {
                    try {
                        ILock lock = targetInstance.getLock(basename + "l" + i.key);
                        try {
                            if (lock.tryLock(tryLockTimeOutMs, TimeUnit.MILLISECONDS)) {
                                locked.add(i);
                                counter.locked++;
                            }
                        } catch (Exception e) {
                            log.severe(basename + ": trying lock=" + i.key, e);
                            if(throwException){
                                throw new RuntimeException(e);
                            }
                        }
                    } catch (Exception e) {
                        log.severe(basename + ": getting lock for locking=" + i.key, e);
                        if(throwException){
                            throw new RuntimeException(e);
                        }
                    }
                }

                for (KeyIncrementPair i : locked) {
                    try {
                        IList<Long> accounts = targetInstance.getList(basename);
                        long value = accounts.get(i.key);
                        accounts.set(i.key, value + i.inc);
                        localIncrements[i.key] += i.inc;
                        counter.inced++;
                    } catch (Exception e) {
                        log.severe(basename + ": updating account=" + i, e);
                        if(throwException){
                            throw new RuntimeException(e);
                        }
                    }
                }

                int unlockAttempts = 0;
                while (!locked.isEmpty()) {
                    Iterator<KeyIncrementPair> it = locked.iterator();
                    while (it.hasNext()) {
                        KeyIncrementPair i = it.next();
                        try {
                            ILock lock = targetInstance.getLock(basename + "l" + i.key);
                            try {
                                lock.unlock();
                                counter.unlocked++;
                                it.remove();
                            } catch (Exception e) {
                                log.severe(basename + ": unlocking lock =" + i.key, e);
                                if(throwException){
                                    throw new RuntimeException(e);
                                }
                            }
                        } catch (Exception e) {
                            log.severe(basename + ": getting lock for unlocking=" + i.key, e);
                            if(throwException){
                                throw new RuntimeException(e);
                            }
                        }
                    }
                    sleepSeconds(1);

                    if (++unlockAttempts > 5) {
                        log.info(basename + ": Cant unlock=" + locked + " unlockAttempts=" + unlockAttempts);
                        break;
                    }
                }
            }
            targetInstance.getList(basename + "res").add(localIncrements);
            targetInstance.getList(basename + "report").add(counter);
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        IList<LockCounter> results = targetInstance.getList(basename + "report");
        LockCounter total = new LockCounter();
        for (LockCounter i : results) {
            total.add(i);
        }
        log.info(basename + ": " + total + " from " + results.size() + " worker threads");

        IList<long[]> allIncrements = targetInstance.getList(basename + "res");
        long expected[] = new long[keyCount];
        for (long[] incs : allIncrements) {
            for (int i = 0; i < incs.length; i++) {
                expected[i] += incs[i];
            }
        }

        IList<Long> accounts = targetInstance.getList(basename);
        int failures = 0;
        for (int k = 0; k < keyCount; k++) {
            if (expected[k] != accounts.get(k)) {
                failures++;
                log.info(basename + ": key=" + k + " expected " + expected[k] + " != " + "actual " + accounts.get(k));
            }
        }

        assertEquals(basename + ": " + failures + " key=>values have been incremented unExpected", 0, failures);
    }
}