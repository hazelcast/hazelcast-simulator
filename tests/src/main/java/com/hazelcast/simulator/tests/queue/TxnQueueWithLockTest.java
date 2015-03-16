package com.hazelcast.simulator.tests.queue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.tests.helpers.TxnCounter;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.utils.ThreadSpawner;
import com.hazelcast.transaction.TransactionContext;

import static org.junit.Assert.assertFalse;

/**
 * This simulator test simulates the issue #2287
 */
public class TxnQueueWithLockTest {

    private static final ILogger log = Logger.getLogger(TxnQueueWithLockTest.class);

    public String basename = this.getClass().getSimpleName();
    public int threadCount = 5;

    private HazelcastInstance instance = null;
    private TestContext testContext = null;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        this.instance = testContext.getTargetInstance();
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
        private TxnCounter counter = new TxnCounter();

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                try {
                    ILock firstLock = instance.getLock(basename + "l1");
                    firstLock.lock();

                    TransactionContext ctx = instance.newTransactionContext();
                    try {
                        ctx.beginTransaction();

                        TransactionalQueue<Integer> queue = ctx.getQueue(basename + "q");
                        queue.offer(1);

                        ILock secondLock = instance.getLock(basename + "l2");
                        secondLock.lock();
                        secondLock.unlock();

                        ctx.commitTransaction();
                        counter.committed++;

                    } catch (Exception txnException) {
                        // TODO: Bad exception handling
                        try {
                            ctx.rollbackTransaction();
                            counter.rolled++;

                            log.severe(basename + ": Exception in txn " + counter, txnException);
                        } catch (Exception rollException) {
                            // TODO: Bad exception handling
                            counter.failedRoles++;
                            log.severe(basename + ": Exception in roll " + counter,rollException);
                        }
                    } finally {
                        firstLock.unlock();
                    }
                } catch (Exception e) {
                    // TODO: Bad exception handling
                    log.warning(e.getMessage(), e);
                }
            }
            IList<TxnCounter> results = instance.getList(basename + "results");
            results.add(counter);
        }
    }

    @Verify(global = true)
    public void verify() {
        IQueue queue = instance.getQueue(basename + "q");
        ILock firstLock = instance.getLock(basename + "l1");
        ILock secondLock = instance.getLock(basename + "l2");

        IList<TxnCounter> results = instance.getList(basename + "results");

        TxnCounter total = new TxnCounter();
        for (TxnCounter counter : results) {
            total.add(counter);
        }

        log.info(basename + ": " + total + " from " + results.size() + " worker Threads  Queue size=" + queue.size());
        assertFalse(basename + ": firstLock.isLocked()", firstLock.isLocked());
        assertFalse(basename + ": secondLock.isLocked()", secondLock.isLocked());
        //assertEquals(total.committed - total.rolled, queue.size());
    }

    public static void main(String[] args) throws Throwable {
        TxnQueueWithLockTest test = new TxnQueueWithLockTest();
        new TestRunner<TxnQueueWithLockTest>(test).run();
    }
}