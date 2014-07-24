package com.hazelcast.stabilizer.tests.queue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.queue.helpers.TxnCounter;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import com.hazelcast.transaction.TransactionContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * This stabilizer test simulates the issue #2287
 */
public class TxnQueueWithLockTest {

    public String basename = this.getClass().getName();
    public int threadCount = 5;

    private HazelcastInstance instance=null;
    private TestContext testContext = null;
    private ILock firstLock = null;
    private ILock secondLock = null;
    private IQueue queue = null;
    private IList<TxnCounter> results = null;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        this.instance = testContext.getTargetInstance();

        firstLock = instance.getLock(basename +"lock1");
        secondLock = instance.getLock(basename +"lock2");
        queue = instance.getQueue(basename +"q");
        results =  instance.getList(basename +"results");
    }

    @Teardown
    public void teardown() throws Exception {
        firstLock.destroy();
        secondLock.destroy();
        queue.destroy();
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
        private TxnCounter  counter = new TxnCounter();

        @Override
        public void run() {
            while (!testContext.isStopped()) {

                firstLock.lock();
                TransactionContext ctx = instance.newTransactionContext();
                ctx.beginTransaction();
                try {
                    TransactionalQueue<Integer> queue = ctx.getQueue(basename +"q");

                    queue.offer(1);
                    secondLock.lock();
                    secondLock.unlock();

                    ctx.commitTransaction();
                    counter.committed++;

                } catch (Exception e) {
                    ctx.rollbackTransaction();
                    counter.rolled++;
                } finally {
                    firstLock.unlock();
                }
            }
            results.add(counter);
        }
    }

    @Verify(global = true)
    public void verify() {
        TxnCounter  total = new TxnCounter();
        for(TxnCounter counter : results){
            total.add(counter);
        }

        System.out.println(basename +":"+ total);
        assertFalse(firstLock.isLocked());
        assertFalse(secondLock.isLocked());
        assertEquals(total.committed - total.rolled, queue.size());
    }

    public static void main(String[] args) throws Throwable {
        TxnQueueWithLockTest test = new TxnQueueWithLockTest();
        new TestRunner(test).run();
    }
}