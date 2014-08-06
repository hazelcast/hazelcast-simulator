package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.helpers.TxnCounter;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;

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
                try{
                    TransactionContext context = targetInstance.newTransactionContext();
                    try{
                        context.beginTransaction();
                        final TransactionalMap<Integer, Long> map = context.getMap(basename);
                        final int key = random.nextInt(keyCount);
                        final long increment = random.nextInt(100);

                        Long current = map.getForUpdate(key);
                        Long update = current + increment;
                        map.put(key, update);

                        context.commitTransaction();
                        localIncrements[key]+=increment;
                        count.committed++;

                    }catch(TransactionException e){
                        context.rollbackTransaction();
                        count.rolled++;
                        System.out.println(basename+": "+e);
                    }
                }catch(TargetDisconnectedException e){
                    System.out.println(basename+": "+e);
                }catch(HazelcastInstanceNotActiveException e){
                    System.out.println(basename+": "+e);
                }
            }
            targetInstance.getList(basename+"res").add(localIncrements);
            targetInstance.getList(basename+"report").add(count);
        }
    }


    @Verify(global = true)
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
        System.out.println(basename+": received increments from "+allIncrements.size()+" workers" );


        IMap<Integer, Long> map = targetInstance.getMap(basename);

        int failures = 0;
        for (int k = 0; k < keyCount; k++) {
            if (expected[k] != map.get(k)) {
                failures++;
            }
        }

        assertEquals(basename+": "+failures+" key=>values have been incremented unExpected", 0, failures);
    }

}

