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
package com.hazelcast.simulator.tests.queue;

import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.TxnCounter;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalQueue;

import static org.junit.Assert.assertFalse;

/**
 * This simulator test simulates the issue #2287
 */
public class TxnQueueWithLockTest extends HazelcastTest {

    private FencedLock firstLock;
    private FencedLock secondLock;

    @Setup
    public void setup() {
        firstLock = targetInstance.getCPSubsystem().getLock(name + "l1");
        secondLock = targetInstance.getCPSubsystem().getLock(name + "l2");
    }

    @TimeStep
    public void timeStep(ThreadState state) {
        firstLock.lock();
        try {
            TransactionContext ctx = targetInstance.newTransactionContext();
            try {
                ctx.beginTransaction();

                TransactionalQueue<Integer> queue = ctx.getQueue(name + 'q');
                queue.offer(1);

                secondLock.lock();
                secondLock.unlock();

                queue.take();

                ctx.commitTransaction();
                state.counter.committed++;
            } catch (Exception txnException) {
                try {
                    ctx.rollbackTransaction();
                    state.counter.rolled++;

                    logger.fatal(name + ": Exception in txn " + state.counter, txnException);
                } catch (Exception rollException) {
                    state.counter.failedRollbacks++;
                    logger.fatal(name + ": Exception in roll " + state.counter, rollException);
                }
            }
        } catch (Exception e) {
            logger.fatal(name + ": outer Exception" + state.counter, e);
        } finally {
            firstLock.unlock();
        }
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        IList<TxnCounter> results = targetInstance.getList(name + "results");
        results.add(state.counter);
    }

    public class ThreadState extends BaseThreadState {
        private TxnCounter counter = new TxnCounter();
    }

    @Verify
    public void globalVerify() {
        IQueue queue = targetInstance.getQueue(name + 'q');
        IList<TxnCounter> results = targetInstance.getList(name + "results");

        TxnCounter total = new TxnCounter();
        for (TxnCounter counter : results) {
            total.add(counter);
        }

        logger.info(name + ": " + total + " from " + results.size() + " worker Threads  Queue size=" + queue.size());
        assertFalse(name + ": firstLock.isLocked()", firstLock.isLocked());
        assertFalse(name + ": secondLock.isLocked()", secondLock.isLocked());
        // TODO: check if this assert can be re-enabled: assertEquals(total.committed - total.rolled, queue.size())
    }
}
