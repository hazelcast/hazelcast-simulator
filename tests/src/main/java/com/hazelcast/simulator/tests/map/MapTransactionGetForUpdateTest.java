/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.simulator.tests.helpers.TxnCounter;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.rethrow;
import static org.junit.Assert.assertEquals;

/**
 * In this test we are using a TransactionContext and starting and committing a Transaction to control concurrent access
 * to a TransactionalMap. this test is incrementing the key value pairs of a map and keeping track of all successful
 * increments to each key.  In the end we verify that for each key value pair the a value in the map matches the increments
 * done on that key,
 */
public class MapTransactionGetForUpdateTest {

    private static final ILogger LOGGER = Logger.getLogger(MapTransactionGetForUpdateTest.class);

    // properties
    public String basename = MapTransactionGetForUpdateTest.class.getSimpleName();
    public int threadCount = 40;
    public int keyCount = 100;
    public boolean rethrowAllException = false;
    public boolean rethrowRollBackException = false;
    public TransactionOptions.TransactionType transactionType = TransactionOptions.TransactionType.TWO_PHASE;
    public int durability = 1;

    private HazelcastInstance targetInstance;
    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
    }

    @Warmup(global = true)
    public void warmup() {
        IMap<Integer, Long> map = targetInstance.getMap(basename);
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0L);
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(basename);
        for (int i = 0; i < threadCount; i++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {

        private final Random random = new Random();
        private final long[] localIncrements = new long[keyCount];
        private TxnCounter count = new TxnCounter();

        @Override
        @SuppressWarnings("PMD.PreserveStackTrace")
        public void run() {
            TransactionOptions options = new TransactionOptions()
                    .setTransactionType(transactionType)
                    .setDurability(durability);

            while (!testContext.isStopped()) {
                TransactionContext context = null;

                final int key = random.nextInt(keyCount);
                final long increment = random.nextInt(100);
                try {
                    context = targetInstance.newTransactionContext(options);
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
                            LOGGER.warning(basename + ": commit failed key=" + key + " inc=" + increment, commitFailedException);
                            if (rethrowAllException) {
                                throw rethrow(commitFailedException);
                            }

                            context.rollbackTransaction();
                            count.rolled++;
                        } catch (Exception rollBackFailedException) {
                            LOGGER.warning(basename + ": rollback failed key=" + key + " inc=" + increment,
                                    rollBackFailedException);
                            count.failedRollbacks++;

                            if (rethrowRollBackException) {
                                throw rethrow(rollBackFailedException);
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
    public void verify() {
        IList<TxnCounter> counts = targetInstance.getList(basename + "report");
        TxnCounter total = new TxnCounter();
        for (TxnCounter c : counts) {
            total.add(c);
        }
        LOGGER.info(basename + ": " + total + " from " + counts.size() + " workers");

        IList<long[]> allIncrements = targetInstance.getList(basename + "res");
        long[] expected = new long[keyCount];
        for (long[] incs : allIncrements) {
            for (int i = 0; i < incs.length; i++) {
                expected[i] += incs[i];
            }
        }

        IMap<Integer, Long> map = targetInstance.getMap(basename);

        int failures = 0;
        for (int i = 0; i < keyCount; i++) {
            if (expected[i] != map.get(i)) {
                failures++;
                LOGGER.info(basename + ": key=" + i + " expected " + expected[i] + " != " + "actual " + map.get(i));
            }
        }

        assertEquals(basename + ": " + failures + " key=>values have been incremented unExpected", 0, failures);
    }
}
