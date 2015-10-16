package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;

import static org.junit.Assert.assertEquals;

/**
 * In this test we execute a TransactionalTask to control the access to a TransactionalMap.
 * there are a total of keyCount keys stored in a map which are initialized to zero,
 * we concurrently increment the value of a random key.  We keep track of all increments to each key and verify
 * the value in the map for each key is equal to the total increments done on each key.
 */
public class MapTransactionTest {

    private static final ILogger LOGGER = Logger.getLogger(MapTransactionTest.class);

    // properties
    public String basename = getClass().getSimpleName();
    public int keyCount = 1000;
    public boolean reThrowTransactionException = false;
    public TransactionType transactionType = TransactionType.TWO_PHASE;
    public int durability = 1;
    public boolean getForUpdate = true;

    private HazelcastInstance targetInstance;
    private IMap<Integer, Long> map;
    private IList<long[]> resultList;
    private TransactionOptions transactionOptions;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
        resultList = targetInstance.getList(basename + "results");

        transactionOptions = new TransactionOptions();
        transactionOptions.setTransactionType(transactionType).setDurability(durability);
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0L);
        }
    }

    @Verify(global = true)
    public void verify() {
        long[] total = new long[keyCount];

        LOGGER.info(basename + ": collected increments from " + resultList.size() + " worker threads");

        for (long[] increments : resultList) {
            for (int i = 0; i < increments.length; i++) {
                total[i] += increments[i];
            }
        }

        int failures = 0;
        for (int i = 0; i < keyCount; i++) {
            if (total[i] != map.get(i)) {
                failures++;
                LOGGER.info(basename + ": key=" + i + " expected val " + total[i] + " !=  map val" + map.get(i));
            }
        }

        assertEquals(basename + ": " + failures + " keys have been incremented unexpectedly out of " + keyCount + " keys",
                0, failures);
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        private final long[] increments = new long[keyCount];

        @Override
        protected void timeStep() throws Exception {
            final int key = randomInt(keyCount);
            final int increment = randomInt(100);

            try {
                targetInstance.executeTransaction(transactionOptions, new TransactionalTask<Object>() {
                    @Override
                    public Object execute(TransactionalTaskContext txContext) {
                        TransactionalMap<Integer, Long> txMap = txContext.getMap(basename);
                        Long value;
                        if (getForUpdate) {
                            value = txMap.getForUpdate(key);
                        } else {
                            value = txMap.get(key);
                        }
                        txMap.put(key, value + increment);
                        return null;
                    }
                });
                increments[key] += increment;
            } catch (TransactionException e) {
                if (reThrowTransactionException) {
                    throw new TestException(e);
                }
                LOGGER.warning(basename + ": caught TransactionException ", e);
            }
        }

        @Override
        protected void afterRun() {
            resultList.add(increments);
        }
    }
}
