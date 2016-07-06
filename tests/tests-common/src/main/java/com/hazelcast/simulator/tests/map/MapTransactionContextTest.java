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

import com.hazelcast.core.TransactionalMap;
import com.hazelcast.simulator.test.BaseThreadContext;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.AbstractTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.rethrow;

public class MapTransactionContextTest extends AbstractTest {

    // properties
    public TransactionType transactionType = TransactionType.TWO_PHASE;
    public int durability = 1;
    public int range = 1000;
    public boolean failOnException = true;

    @TimeStep
    public void timeStep(ThreadContext context) {
        int key = context.nextRandom(0, range / 2);

        TransactionOptions transactionOptions = new TransactionOptions()
                .setTransactionType(transactionType)
                .setDurability(durability);

        TransactionContext transactionContext = targetInstance.newTransactionContext(transactionOptions);

        transactionContext.beginTransaction();

        TransactionalMap<Object, Object> txMap = transactionContext.getMap("map");

        try {
            Object val = txMap.getForUpdate(key);

            if (val != null) {
                key = context.nextRandom(range / 2, range);
            }

            txMap.put(key, (long) key);

            transactionContext.commitTransaction();
        } catch (Exception e) {
            logger.severe("----------------------tx exception -------------------------", e);

            if (failOnException) {
                throw rethrow(e);
            }

            transactionContext.rollbackTransaction();
        }
    }

    public class ThreadContext extends BaseThreadContext {

        private int nextRandom(int min, int max) {
            return random.nextInt(max - min) + min;
        }
    }
}
