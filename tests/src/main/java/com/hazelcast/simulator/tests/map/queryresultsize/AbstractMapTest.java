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
package com.hazelcast.simulator.tests.map.queryresultsize;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.QueryResultSizeLimiter;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.tests.helpers.HazelcastTestUtils;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import com.hazelcast.simulator.worker.tasks.IWorker;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getNode;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.String.format;
import static org.junit.Assert.fail;

abstract class AbstractMapTest {

    private static final ILogger LOGGER = Logger.getLogger(AbstractMapTest.class);

    HazelcastInstance hazelcastInstance;
    GroupProperties groupProperties;
    IMap<Object, Object> map;
    IAtomicLong operationCounter;
    IAtomicLong exceptionCounter;
    String basename;

    long globalKeyCount;
    int localKeyCount;

    void failOnVersionMismatch() {
        HazelcastTestUtils.failOnVersionMismatch("3.5", basename + ": This tests needs Hazelcast %s or newer");
    }

    void failIfFeatureDisabled() {
        if (groupProperties.QUERY_RESULT_SIZE_LIMIT.getInteger() <= 0) {
            fail(basename + ": QueryResultSizeLimiter is disabled");
        }
    }

    void baseSetup(TestContext testContext, String basename) {
        this.hazelcastInstance = testContext.getTargetInstance();

        this.groupProperties = getNode(hazelcastInstance).getGroupProperties();
        this.map = hazelcastInstance.getMap(basename);
        this.operationCounter = hazelcastInstance.getAtomicLong(basename + "Ops");
        this.exceptionCounter = hazelcastInstance.getAtomicLong(basename + "Exceptions");
        this.basename = basename;

        Integer minResultSizeLimit = 100000;
        Float resultLimitFactor = 1.15f;
        try {
            minResultSizeLimit = QueryResultSizeLimiter.MINIMUM_MAX_RESULT_LIMIT;
            resultLimitFactor = QueryResultSizeLimiter.MAX_RESULT_LIMIT_FACTOR;

            LOGGER.info(format("%s: QueryResultSizeLimiter is configured with limit %d and pre-check partition limit %d",
                    basename,
                    groupProperties.QUERY_RESULT_SIZE_LIMIT.getInteger(),
                    groupProperties.QUERY_MAX_LOCAL_PARTITION_LIMIT_FOR_PRE_CHECK.getInteger()));
        } catch (Throwable e) {
            LOGGER.info(format("%s: QueryResultSizeLimiter is not implemented in this Hazelcast version", basename));
        }

        int clusterSize = hazelcastInstance.getCluster().getMembers().size();
        this.globalKeyCount = getGlobalKeyCount(minResultSizeLimit, resultLimitFactor);
        this.localKeyCount = (int) Math.ceil(globalKeyCount / (double) clusterSize);

        LOGGER.info(format("%s: Filling map with %d items (%d items per member, %d members in cluster)",
                basename, globalKeyCount, localKeyCount, clusterSize));
    }

    abstract long getGlobalKeyCount(Integer minResultSizeLimit, Float resultLimitFactor);

    void baseWarmup(String keyType) {
        if (!isMemberNode(hazelcastInstance)) {
            return;
        }

        if ("String".equals(keyType)) {
            baseWarmupStringKey();
        } else if ("Integer".equals(keyType)) {
            baseWarmupIntKey();
        } else {
            throw new IllegalArgumentException("Unknown key type " + keyType);
        }
    }

    void baseWarmupStringKey() {
        String[] keys = generateStringKeys(localKeyCount, 10, KeyLocality.LOCAL, hazelcastInstance);
        int i = 0;
        for (String key : keys) {
            map.put(key, i++);
        }
    }

    void baseWarmupIntKey() {
        int[] keys = generateIntKeys(localKeyCount, Integer.MAX_VALUE, KeyLocality.LOCAL, hazelcastInstance);
        int i = 0;
        for (int key : keys) {
            map.put(key, i++);
        }
    }

    IWorker baseRunWithWorker(String operationType) {
        if ("values".equals(operationType)) {
            return new ValuesWorker();
        } else if ("keySet".equals(operationType)) {
            return new KeySetWorker();
        } else if ("entrySet".equals(operationType)) {
            return new EntrySetWorker();
        } else {
            throw new IllegalArgumentException("Unknown operation type: " + operationType);
        }
    }

    private abstract class BaseWorker extends AbstractMonotonicWorker {

        protected long localOperationCounter;
        protected long localExceptionCounter;

        @Override
        protected void timeStep() {
            localOperationCounter++;
            try {
                mapOperation();
            } catch (Exception e) {
                if ("QueryResultSizeExceededException".equals(e.getClass().getSimpleName())) {
                    localExceptionCounter++;
                } else {
                    throw rethrow(e);
                }
            }
        }

        protected abstract void mapOperation();

        @Override
        protected void afterRun() {
            operationCounter.addAndGet(localOperationCounter);
            exceptionCounter.addAndGet(localExceptionCounter);
        }
    }

    class ValuesWorker extends BaseWorker {

        @Override
        protected void mapOperation() {
            map.values();
        }
    }

    class KeySetWorker extends BaseWorker {

        @Override
        protected void mapOperation() {
            map.keySet();
        }
    }

    class EntrySetWorker extends BaseWorker {

        @Override
        protected void mapOperation() {
            map.entrySet();
        }
    }
}
