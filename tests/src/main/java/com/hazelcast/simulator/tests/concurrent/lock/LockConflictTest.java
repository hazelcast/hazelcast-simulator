/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.KeyIncrementPair;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.rethrow;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static org.junit.Assert.assertEquals;

// TODO: We need to deal with exception logging; they are logged but not visible to Simulator
public class LockConflictTest {

    private static final ILogger LOGGER = Logger.getLogger(LockConflictTest.class);

    // properties
    public String basename = LockConflictTest.class.getSimpleName();
    public int keyCount = 50;
    public int maxKeysPerTxn = 5;
    public int tryLockTimeOutMs = 10;
    public boolean throwException = false;

    private HazelcastInstance hazelcastInstance;
    private IList<Long> list;

    private IList<long[]> globalIncrements;
    private IList<LockCounter> globalCounter;

    @Setup
    public void setup(TestContext testContext) {
        hazelcastInstance = testContext.getTargetInstance();
        list = hazelcastInstance.getList(basename);

        globalIncrements = hazelcastInstance.getList(basename + "res");
        globalCounter = hazelcastInstance.getList(basename + "report");
    }

    @Warmup(global = true)
    public void warmup() {
        for (int i = 0; i < keyCount; i++) {
            list.add(0L);
        }
    }

    @Verify(global = false)
    public void verify() {
        LockCounter total = new LockCounter();
        for (LockCounter counter : globalCounter) {
            total.add(counter);
        }
        LOGGER.info(basename + ": " + total + " from " + globalCounter.size() + " worker threads");

        long[] expected = new long[keyCount];
        for (long[] increments : globalIncrements) {
            for (int i = 0; i < increments.length; i++) {
                expected[i] += increments[i];
            }
        }

        int failures = 0;
        for (int key = 0; key < keyCount; key++) {
            if (expected[key] != list.get(key)) {
                failures++;
                LOGGER.info(basename + ": key=" + key + " expected " + expected[key] + " != " + "actual " + list.get(key));
            }
        }
        assertEquals(basename + ": " + failures + " key=>values have been incremented unexpected", 0, failures);
    }

    @RunWithWorker
    public Worker run() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        private final LockCounter localCounter = new LockCounter();
        private final long[] localIncrements = new long[keyCount];

        @Override
        protected void timeStep() throws Exception {
            List<KeyIncrementPair> potentialLocks = getPotentialLocks();
            List<KeyIncrementPair> locked = getLocks(potentialLocks);
            incrementLockedValues(locked);
            releaseLocks(locked);
        }

        private List<KeyIncrementPair> getPotentialLocks() {
            List<KeyIncrementPair> potentialLocks = new ArrayList<KeyIncrementPair>();
            for (int i = 0; i < maxKeysPerTxn; i++) {
                potentialLocks.add(new KeyIncrementPair(getRandom(), keyCount, 999));
            }
            return potentialLocks;
        }

        private List<KeyIncrementPair> getLocks(List<KeyIncrementPair> potentialLocks) {
            List<KeyIncrementPair> locked = new ArrayList<KeyIncrementPair>();
            for (KeyIncrementPair keyIncrementPair : potentialLocks) {
                try {
                    ILock lock = getLock(keyIncrementPair);
                    try {
                        if (lock.tryLock(tryLockTimeOutMs, TimeUnit.MILLISECONDS)) {
                            locked.add(keyIncrementPair);
                            localCounter.locked++;
                        }
                    } catch (Exception e) {
                        LOGGER.severe(basename + ": trying lock=" + keyIncrementPair.key, e);
                        if (throwException) {
                            throw rethrow(e);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.severe(basename + ": getting lock for locking=" + keyIncrementPair.key, e);
                    if (throwException) {
                        throw rethrow(e);
                    }
                }
            }
            return locked;
        }

        private void incrementLockedValues(List<KeyIncrementPair> locked) {
            for (KeyIncrementPair keyIncrementPair : locked) {
                try {
                    long value = list.get(keyIncrementPair.key);
                    list.set(keyIncrementPair.key, value + keyIncrementPair.increment);

                    localIncrements[keyIncrementPair.key] += keyIncrementPair.increment;
                    localCounter.increased++;
                } catch (Exception e) {
                    LOGGER.severe(basename + ": updating account=" + keyIncrementPair, e);
                    if (throwException) {
                        throw rethrow(e);
                    }
                }
            }
        }

        private void releaseLocks(List<KeyIncrementPair> locked) {
            int unlockAttempts = 0;
            while (!locked.isEmpty()) {
                Iterator<KeyIncrementPair> iterator = locked.iterator();
                while (iterator.hasNext()) {
                    KeyIncrementPair keyIncrementPair = iterator.next();
                    try {
                        ILock lock = getLock(keyIncrementPair);
                        try {
                            lock.unlock();
                            localCounter.unlocked++;
                            iterator.remove();
                        } catch (Exception e) {
                            LOGGER.severe(basename + ": unlocking lock =" + keyIncrementPair.key, e);
                            if (throwException) {
                                throw rethrow(e);
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.severe(basename + ": getting lock for unlocking=" + keyIncrementPair.key, e);
                        if (throwException) {
                            throw rethrow(e);
                        }
                    }
                }
                sleepSeconds(1);

                if (++unlockAttempts > 5) {
                    LOGGER.info(basename + ": Cant unlock=" + locked + " unlockAttempts=" + unlockAttempts);
                    break;
                }
            }
        }

        private ILock getLock(KeyIncrementPair keyIncrementPair) {
            return hazelcastInstance.getLock(basename + 'l' + keyIncrementPair.key);
        }

        @Override
        protected void afterRun() {
            globalIncrements.add(localIncrements);
            globalCounter.add(localCounter);
        }
    }
}
