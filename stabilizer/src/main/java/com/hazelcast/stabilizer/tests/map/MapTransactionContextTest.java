package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.*;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.helpers.TxnCounter;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import com.hazelcast.transaction.TransactionContext;

import java.util.Random;

import static org.junit.Assert.assertEquals;

public class MapTransactionContextTest {

    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int keyCount = 10;

    private HazelcastInstance targetInstance;
    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        IMap<Integer, Long> map = targetInstance.getMap(basename);
        for (int k = 0; k < keyCount; k++) {
            map.put(k, 0l);
        }

        /*
        try {
            PartitionService ps = targetInstance.getPartitionService();
            // At most wait about 5 minutes, possibly a little bit more :)
            // since execution time of "isClusterSafe()" is not zero
            // but who cares :)
            for (int i = 0; i < 5 * 60; i++) {
                if (ps.isClusterSafe()) {
                    break;
                }
                Thread.sleep(1000);
            }
        } catch (Throwable t) {}
        */

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
        private TxnCounter count = new TxnCounter();

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                TransactionContext context = null;

                final int key = random.nextInt(keyCount);
                final long increment = random.nextInt(100);
                try {
                    context = targetInstance.newTransactionContext();

                    context.beginTransaction();

                    final TransactionalMap<Integer, Long> map = context.getMap(basename);

                    Long current = map.getForUpdate(key);
                    Long update = current + increment;
                    map.put(key, update);

                    context.commitTransaction();

                    // Do local increments if commit is successful, so there is no needed decrement operation
                    localIncrements[key]+=increment;
                    count.committed++;
                } catch (Exception commitFailed) {
                    if (context != null) {
                        try {
                            context.rollbackTransaction();
                            count.rolled++;

                            System.out.println(basename + ": commit   fail key=" + key + " inc=" + increment + " " + commitFailed);
                            commitFailed.printStackTrace();
                        } catch (Exception rollBackFailed) {
                            count.failedRoles++;

                            System.out.println(basename + ": rollback fail key=" + key + " inc=" + increment + " " + rollBackFailed);
                            rollBackFailed.printStackTrace();
                        }
                    }
                }
            }
            targetInstance.getList(basename+"res").add(localIncrements);
            targetInstance.getList(basename+"report").add(count);
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        IList<TxnCounter> counts = targetInstance.getList(basename+"report");
        TxnCounter total = new TxnCounter();
        for(TxnCounter c : counts){
            total.add(c);
        }
        System.out.println(basename + ": "+total +" from "+counts.size()+" workers");

        IList<long[]> allIncrements = targetInstance.getList(basename+"res");
        long expected[] = new long[keyCount];
        for (long[] incs : allIncrements) {
            for (int i=0; i < incs.length; i++) {
                expected[i] += incs[i];
            }
        }

        IMap<Integer, Long> map = targetInstance.getMap(basename);

        int failures = 0;
        for (int k = 0; k < keyCount; k++) {
            if (expected[k] != map.get(k)) {
                failures++;

                System.out.println(basename+": key="+k+" expected "+expected[k]+" != " +"actual "+map.get(k));
            }
        }

        assertEquals(basename+": "+failures+" key=>values have been incremented unExpected", 0, failures);
    }

}
