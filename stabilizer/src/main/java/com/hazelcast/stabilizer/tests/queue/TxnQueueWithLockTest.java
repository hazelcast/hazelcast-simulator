package com.hazelcast.stabilizer.tests.queue;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.spi.exception.TargetDisconnectedException;
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
        private TxnCounter  counter = new TxnCounter();

        @Override
        public void run() {
            while (!testContext.isStopped()) {

                try{
                    ILock firstLock = instance.getLock(basename +"l1");
                    firstLock.lock();

                    TransactionContext ctx = instance.newTransactionContext();
                    ctx.beginTransaction();

                    try {
                        TransactionalQueue<Integer> queue = ctx.getQueue(basename +"q");

                        queue.offer(1);

                        ILock secondLock = instance.getLock(basename +"l2");
                        secondLock.lock();
                        secondLock.unlock();

                        ctx.commitTransaction();
                        counter.committed++;

                    } catch (Exception e) {
                        ctx.rollbackTransaction();
                        counter.rolled++;

                        System.out.println(basename+": ThreadLocal txn No. "+ counter.committed+1+" ThreadLocal roles ="+counter.rolled);
                        System.out.println(basename+": "+e);

                    } finally {
                        firstLock.unlock();
                    }
                }catch(TargetDisconnectedException e){
                    System.out.println(e);
                }catch(HazelcastInstanceNotActiveException e){
                    System.out.println(e);
                }
            }
            IList<TxnCounter> results =  instance.getList(basename +"results");
            results.add(counter);
        }
    }

    @Verify(global = true)
    public void verify() {

        IQueue queue = instance.getQueue(basename +"q");
        ILock firstLock = instance.getLock(basename +"l1");
        ILock secondLock = instance.getLock(basename +"l2");


        IList<TxnCounter> results =  instance.getList(basename +"results");

        TxnCounter  total = new TxnCounter();
        for(TxnCounter counter : results){
            total.add(counter);
        }

        System.out.println(basename +": "+ total+" from "+results.size());
        assertFalse(firstLock.isLocked());
        assertFalse(secondLock.isLocked());
        assertEquals(total.committed - total.rolled, queue.size());
    }

    public static void main(String[] args) throws Throwable {
        TxnQueueWithLockTest test = new TxnQueueWithLockTest();
        new TestRunner(test).run();
    }
}