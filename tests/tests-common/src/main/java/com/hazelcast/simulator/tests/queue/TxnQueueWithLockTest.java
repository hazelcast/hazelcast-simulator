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

import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.TxnCounter;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.transaction.TransactionContext;

import static org.junit.Assert.assertFalse;

/**
 * This simulator test simulates the issue #2287
 */
public class TxnQueueWithLockTest extends AbstractTest {

    public int threadCount = 5;

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(name);
        for (int i = 0; i < threadCount; i++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        private TxnCounter counter = new TxnCounter();

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                try {
                    ILock firstLock = targetInstance.getLock(name + "l1");
                    firstLock.lock();

                    TransactionContext ctx = targetInstance.newTransactionContext();
                    try {
                        ctx.beginTransaction();

                        TransactionalQueue<Integer> queue = ctx.getQueue(name + 'q');
                        queue.offer(1);

                        ILock secondLock = targetInstance.getLock(name + "l2");
                        secondLock.lock();
                        secondLock.unlock();

                        queue.take();

                        ctx.commitTransaction();
                        counter.committed++;

                    } catch (Exception txnException) {
                        try {
                            ctx.rollbackTransaction();
                            counter.rolled++;

                            logger.severe(name + ": Exception in txn " + counter, txnException);
                        } catch (Exception rollException) {
                            counter.failedRollbacks++;
                            logger.severe(name + ": Exception in roll " + counter, rollException);
                        }
                    } finally {
                        firstLock.unlock();
                    }
                } catch (Exception e) {
                    logger.severe(name + ": outer Exception" + counter, e);
                }
            }
            IList<TxnCounter> results = targetInstance.getList(name + "results");
            results.add(counter);
        }
    }

    @Verify
    public void globalVerify() {
        IQueue queue = targetInstance.getQueue(name + 'q');
        ILock firstLock = targetInstance.getLock(name + "l1");
        ILock secondLock = targetInstance.getLock(name + "l2");

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
