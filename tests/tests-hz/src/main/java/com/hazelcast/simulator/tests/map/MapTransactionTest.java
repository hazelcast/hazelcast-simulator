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

import com.hazelcast.collection.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.rethrow;
import static com.hazelcast.transaction.TransactionOptions.TransactionType.TWO_PHASE;
import static org.junit.Assert.assertEquals;

/**
 * In this test we execute a TransactionalTask to control the access to a TransactionalMap.
 * there are a total of keyCount keys stored in a map which are initialized to zero,
 * we concurrently increment the value of a random key.  We keep track of all increments to each key and verify
 * the value in the map for each key is equal to the total increments done on each key.
 */
public class MapTransactionTest extends HazelcastTest {

    // properties
    public int keyCount = 1000;
    public boolean reThrowTransactionException = false;
    public TransactionType transactionType = TWO_PHASE;
    public int durability = 1;
    public boolean getForUpdate = true;

    private IMap<Integer, Long> map;
    private IList<long[]> resultList;
    private TransactionOptions transactionOptions;

    @Setup
    public void setup() {
        map = targetInstance.getMap(name);
        resultList = targetInstance.getList(name + "results");

        transactionOptions = new TransactionOptions();
        transactionOptions.setTransactionType(transactionType).setDurability(durability);
    }

    @Prepare(global = true)
    public void prepare() {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0L);
        }
    }

    @TimeStep
    public void timeStep(ThreadState state) throws Exception {
        final int key = state.randomInt(keyCount);
        final int increment = state.randomInt(100);

        try {
            targetInstance.executeTransaction(transactionOptions, new TransactionalTask<Object>() {
                @Override
                public Object execute(TransactionalTaskContext txContext) {
                    TransactionalMap<Integer, Long> txMap = txContext.getMap(name);
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
            state.increments[key] += increment;
        } catch (TransactionException e) {
            if (reThrowTransactionException) {
                throw rethrow(e);
            }
            logger.warn(name + ": caught TransactionException ", e);
        }
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        resultList.add(state.increments);
    }

    public class ThreadState extends BaseThreadState {
        private final long[] increments = new long[keyCount];
    }

    @Verify(global = true)
    public void verify() {
        long[] total = new long[keyCount];

        logger.info(name + ": collected increments from " + resultList.size() + " worker threads");

        for (long[] increments : resultList) {
            for (int i = 0; i < increments.length; i++) {
                total[i] += increments[i];
            }
        }

        int failures = 0;
        for (int i = 0; i < keyCount; i++) {
            if (total[i] != map.get(i)) {
                failures++;
                logger.info(name + ": key=" + i + " expected val " + total[i] + " !=  map val" + map.get(i));
            }
        }

        assertEquals(name + ": " + failures + " keys have been incremented unexpectedly out of " + keyCount + " keys",
                0, failures);
    }
}
