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
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyIncrementPair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.rethrow;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static org.junit.Assert.assertEquals;

// TODO: We need to deal with exception logging; they are logged but not visible to Simulator
public class LockConflictTest extends HazelcastTest {

    // properties
    public int keyCount = 50;
    public int maxKeysPerTxn = 5;
    public int tryLockTimeOutMs = 10;
    public boolean throwException = false;

    private IList<Long> list;
    private IList<long[]> globalIncrements;
    private IList<LockCounter> globalCounter;

    @Setup
    public void setup() {
        list = targetInstance.getList(name);

        globalIncrements = targetInstance.getList(name + "res");
        globalCounter = targetInstance.getList(name + "report");
    }

    @Prepare(global = true)
    public void prepare() {
        for (int i = 0; i < keyCount; i++) {
            list.add(0L);
        }
    }

    @TimeStep
    public void timeStep(ThreadState state) throws Exception {
        List<KeyIncrementPair> potentialLocks = state.getPotentialLocks();
        List<KeyIncrementPair> locked = state.getLocks(potentialLocks);
        state.incrementLockedValues(locked);
        state.releaseLocks(locked);
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        globalIncrements.add(state.localIncrements);
        globalCounter.add(state.localCounter);
    }

    public class ThreadState extends BaseThreadState {
        private final LockCounter localCounter = new LockCounter();
        private final long[] localIncrements = new long[keyCount];

        private List<KeyIncrementPair> getPotentialLocks() {
            List<KeyIncrementPair> potentialLocks = new ArrayList<KeyIncrementPair>();
            for (int i = 0; i < maxKeysPerTxn; i++) {
                potentialLocks.add(new KeyIncrementPair(random, keyCount, 999));
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
                        logger.fatal(name + ": trying lock=" + keyIncrementPair.key, e);
                        if (throwException) {
                            throw rethrow(e);
                        }
                    }
                } catch (Exception e) {
                    logger.fatal(name + ": getting lock for locking=" + keyIncrementPair.key, e);
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
                    logger.fatal(name + ": updating account=" + keyIncrementPair, e);
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
                            logger.fatal(name + ": unlocking lock =" + keyIncrementPair.key, e);
                            if (throwException) {
                                throw rethrow(e);
                            }
                        }
                    } catch (Exception e) {
                        logger.fatal(name + ": getting lock for unlocking=" + keyIncrementPair.key, e);
                        if (throwException) {
                            throw rethrow(e);
                        }
                    }
                }
                sleepSeconds(1);

                if (++unlockAttempts > 5) {
                    logger.info(name + ": Cant unlock=" + locked + " unlockAttempts=" + unlockAttempts);
                    break;
                }
            }
        }

        private ILock getLock(KeyIncrementPair keyIncrementPair) {
            return targetInstance.getLock(name + 'l' + keyIncrementPair.key);
        }
    }

    @Verify(global = false)
    public void verify() {
        LockCounter total = new LockCounter();
        for (LockCounter counter : globalCounter) {
            total.add(counter);
        }
        logger.info(name + ": " + total + " from " + globalCounter.size() + " worker threads");

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
                logger.info(name + ": key=" + key + " expected " + expected[key] + " != " + "actual " + list.get(key));
            }
        }
        assertEquals(name + ": " + failures + " key=>values have been incremented unexpected", 0, failures);
    }
}
