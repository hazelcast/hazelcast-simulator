package com.hazelcast.stabilizer.tests.issues;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.TransactionalQueue;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.AbstractTest;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.transaction.TransactionContext;

import static com.hazelcast.stabilizer.Utils.sleepSeconds;

//https://github.com/hazelcast/hazelcast/issues/2287
public class Issue2287Test extends AbstractTest {

    private final static ILogger log = Logger.getLogger(Issue2287Test.class);

    private HazelcastInstance targetInstance;
    private ILock lock1;
    private ILock lock2;

    //props we can tinker with
    public int threadCount = 10;

    @Override
    public void localSetup() throws Exception {
        targetInstance = getTargetInstance();
        lock1 = targetInstance.getLock("lock1");
        lock2 = targetInstance.getLock("lock2");
    }

    @Override
    public void createTestThreads() {
        for (int k = 0; k < threadCount; k++) {
            spawn(new Worker());
        }

    }

    private class Worker implements Runnable {

        @Override
        public void run() {
            while (!stopped()) {
                singleRun();
            }
        }

        private void singleRun() {
            try {
                lock1.lock();

                TransactionContext ctx = targetInstance.newTransactionContext();
                ctx.beginTransaction();

                TransactionalQueue<Integer> queue = ctx.getQueue("queue");
                queue.offer(1);

                sleepSeconds(1);
                try {
                    lock2.lock();
                    log.info(Thread.currentThread().getName() + " " + System.currentTimeMillis());
                    sleepSeconds(1);
                } finally {
                    lock2.unlock();
                }
                ctx.commitTransaction();
            } finally {
                lock1.unlock();
            }
            sleepSeconds(1);
        }
    }

    //just for local testing purposes.
    public static void main(String[] args) throws Exception {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz3 = Hazelcast.newHazelcastInstance();

        Issue2287Test test = new Issue2287Test();

        TestRunner testRunner = new TestRunner();
        testRunner.setHazelcastInstance(hz1);
        testRunner.run(test, 60000);
    }
}
