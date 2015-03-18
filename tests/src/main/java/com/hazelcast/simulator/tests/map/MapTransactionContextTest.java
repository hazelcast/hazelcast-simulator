package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.tests.helpers.TxnCounter;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.test.utils.ThreadSpawner;
import com.hazelcast.transaction.TransactionContext;

import java.util.Random;

import static org.junit.Assert.assertEquals;


/**
 * In this test we are using a TransactionContext and starting and committing a Transaction to control concurrent access
 * to a TransactionalMap. this test is incrementing the key value pairs of a map and keeping track of all successful
 * increments to each key.  In the end we verify that for each key value pair the a value in the map matches the increments
 * done on that key,
 */
public class MapTransactionContextTest {

    private static final ILogger log = Logger.getLogger(MapTransactionContextTest.class);

    // properties
    public String basename = this.getClass().getSimpleName();
    public int threadCount = 3;
    public int keyCount = 10;
    public boolean rethrowAllException=false;
    public boolean rethrowRollBackException=false;


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
                    localIncrements[key] += increment;
                    count.committed++;
                } catch (Exception commitFailedException) {
                    if (context != null) {
                        try {
                            log.warning(basename + ": commit   fail key=" + key + " inc=" + increment, commitFailedException);

                            if(rethrowAllException){
                                throw new RuntimeException(commitFailedException);
                            }

                            context.rollbackTransaction();
                            count.rolled++;
                        } catch (Exception rollBackFailed) {
                            log.warning(basename + ": rollback fail key=" + key + " inc=" + increment, rollBackFailed);
                            count.failedRoles++;

                            if(rethrowRollBackException){
                                throw new RuntimeException(commitFailedException);
                            }
                        }
                    }
                }
            }
            targetInstance.getList(basename + "res").add(localIncrements);
            targetInstance.getList(basename + "report").add(count);
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        IList<TxnCounter> counts = targetInstance.getList(basename + "report");
        TxnCounter total = new TxnCounter();
        for (TxnCounter c : counts) {
            total.add(c);
        }
        log.info(basename + ": " + total + " from " + counts.size() + " workers");

        IList<long[]> allIncrements = targetInstance.getList(basename + "res");
        long expected[] = new long[keyCount];
        for (long[] incs : allIncrements) {
            for (int i = 0; i < incs.length; i++) {
                expected[i] += incs[i];
            }
        }

        IMap<Integer, Long> map = targetInstance.getMap(basename);

        int failures = 0;
        for (int k = 0; k < keyCount; k++) {
            if (expected[k] != map.get(k)) {
                failures++;
                log.info(basename + ": key=" + k + " expected " + expected[k] + " != " + "actual " + map.get(k));
            }
        }

        assertEquals(basename + ": " + failures + " key=>values have been incremented unExpected", 0, failures);
    }
}