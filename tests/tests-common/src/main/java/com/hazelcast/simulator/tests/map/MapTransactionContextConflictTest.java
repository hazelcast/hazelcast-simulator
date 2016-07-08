/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.simulator.test.BaseThreadContext;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.AbstractTest;
import com.hazelcast.simulator.tests.helpers.KeyIncrementPair;
import com.hazelcast.simulator.tests.helpers.TxnCounter;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.rethrow;
import static org.junit.Assert.assertEquals;

/**
 * Testing transaction context with multi keys.
 * <p>
 * A number of map keys (maxKeysPerTxn) are chosen at random to take part in the transaction. As maxKeysPerTxn increases as a
 * proportion of keyCount, more conflict will occur between the transactions, less transactions will be committed successfully and
 * more transactions are rolled back.
 */
public class MapTransactionContextConflictTest extends AbstractTest {

    private static final int MAX_INCREMENT = 999;

    //todo: this property is not used anymore in this test.
    public int threadCount = 3;
    public int keyCount = 50;
    public int maxKeysPerTxn = 5;
    public boolean throwCommitException = false;
    public boolean throwRollBackException = false;

    @Warmup(global = true)
    public void warmup() {
        IMap<Integer, Long> map = targetInstance.getMap(name);
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0L);
        }
    }

    @TimeStep
    public void timeStep(ThreadContext threadCtx) {
        List<KeyIncrementPair> potentialIncrements = new ArrayList<KeyIncrementPair>();
        for (int i = 0; i < maxKeysPerTxn; i++) {
            KeyIncrementPair p = new KeyIncrementPair(threadCtx.random, keyCount, MAX_INCREMENT);
            potentialIncrements.add(p);
        }

        List<KeyIncrementPair> putIncrements = new ArrayList<KeyIncrementPair>();

        TransactionContext transactionContext = targetInstance.newTransactionContext();
        try {
            transactionContext.beginTransaction();
            TransactionalMap<Integer, Long> map = transactionContext.getMap(name);

            for (KeyIncrementPair p : potentialIncrements) {
                long current = map.getForUpdate(p.key);
                map.put(p.key, current + p.increment);

                putIncrements.add(p);
            }
            transactionContext.commitTransaction();

            // Do local key increments if commit is successful
            threadCtx.count.committed++;
            for (KeyIncrementPair p : putIncrements) {
                threadCtx.localIncrements[p.key] += p.increment;
            }
        } catch (TransactionException commitException) {
            logger.warning(name + ": commit failed. tried key increments=" + putIncrements, commitException);
            if (throwCommitException) {
                throw rethrow(commitException);
            }

            try {
                transactionContext.rollbackTransaction();
                threadCtx.count.rolled++;
            } catch (TransactionException rollBackException) {
                logger.warning(name + ": rollback failed " + rollBackException.getMessage(), rollBackException);
                threadCtx.count.failedRollbacks++;

                if (throwRollBackException) {
                    throw rethrow(rollBackException);
                }
            }
        }
    }

    @AfterRun
    public void afterRun(ThreadContext context) {
        targetInstance.getList(name + "inc").add(context.localIncrements);
        targetInstance.getList(name + "count").add(context.count);
    }

    private class ThreadContext extends BaseThreadContext {
        private final long[] localIncrements = new long[keyCount];
        private final TxnCounter count = new TxnCounter();
    }

    @Verify(global = false)
    public void verify() {
        IList<TxnCounter> counts = targetInstance.getList(name + "count");
        TxnCounter total = new TxnCounter();
        for (TxnCounter c : counts) {
            total.add(c);
        }
        logger.info(name + ": " + total + " from " + counts.size() + " worker threads");

        IList<long[]> allIncrements = targetInstance.getList(name + "inc");
        long[] expected = new long[keyCount];
        for (long[] incs : allIncrements) {
            for (int i = 0; i < incs.length; i++) {
                expected[i] += incs[i];
            }
        }

        IMap<Integer, Long> map = targetInstance.getMap(name);
        int failures = 0;
        for (int i = 0; i < keyCount; i++) {
            if (expected[i] != map.get(i)) {
                failures++;
                logger.info(name + ": key=" + i + " expected " + expected[i] + " != " + "actual " + map.get(i));
            }
        }
        assertEquals(name + ": " + failures + " key=>values have been incremented unExpected", 0, failures);
    }
}
