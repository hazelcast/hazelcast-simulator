package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.test.utils.ThreadSpawner;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * In this test we execute a TransactionalTask to control the access to a TransactionalMap.
 * there are a total of keyCount keys stored in a map which are initialized to zero,
 * we concurrently increment the value of a random key.  We keep track of all increments to each key and verify
 * the value in the map for each key is equal to the total increments done on each key.
 */
public class MapTransactionTest {

    private static final ILogger log = Logger.getLogger(MapTransactionTest.class);

    // properties
    public String basename = this.getClass().getSimpleName();
    public int threadCount = 5;
    public int keyCount = 1000;

    public boolean reThrowTransactionException=false;

    private HazelcastInstance targetInstance;
    private TestContext testContext;
    private int maxInc = 100;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        IMap map = targetInstance.getMap(basename);
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
        private final long[] increments = new long[keyCount];

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                final int key = random.nextInt(keyCount);
                final int increment = random.nextInt(maxInc);

                try {
                    targetInstance.executeTransaction(new TransactionalTask<Object>() {
                        @Override
                        public Object execute(TransactionalTaskContext txContext) throws TransactionException {
                            TransactionalMap<Integer, Long> map = txContext.getMap(basename);
                            Long current = map.getForUpdate(key);
                            map.put(key, current + increment);
                            return null;
                        }
                    });
                    increments[key] += increment;
                } catch (TransactionException e) {
                    if(reThrowTransactionException){
                        throw new RuntimeException(e);
                    }
                    log.warning(basename + ": caught TransactionException ", e);
                }
            }
            IList<long[]> results = targetInstance.getList(basename + "results");
            results.add(increments);
        }
    }

    @Verify(global = true)
    public void verify() throws Exception {
        IList<long[]> allIncrements = targetInstance.getList(basename + "results");
        long[] total = new long[keyCount];

        log.info(basename + ": collected increments from " + allIncrements.size() + " worker threads");

        for (long[] increments : allIncrements) {
            for (int i = 0; i < increments.length; i++) {
                total[i] += increments[i];
            }
        }

        int failures = 0;
        for (int i = 0; i < keyCount; i++) {
            IMap<Integer, Long> map = targetInstance.getMap(basename);
            if (total[i] != map.get(i)) {
                failures++;
                log.info(basename + ": key=" + i + " expected val " + total[i] + " !=  map val" + map.get(i));
            }
        }

        assertEquals(basename + ": " + failures + " keys have been incremented unexpectedly out of " + keyCount + " keys", 0, failures);
    }

}

