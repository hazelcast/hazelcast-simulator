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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.tests.helpers.HazelcastTestUtils;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import com.hazelcast.simulator.worker.tasks.IWorker;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.logPartitionStatistics;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.simulator.utils.ReflectionUtils.getStaticFieldValue;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

abstract class AbstractMapTest {

    protected static final ILogger LOGGER = Logger.getLogger(AbstractMapTest.class);

    HazelcastInstance hazelcastInstance;
    IMap<Object, Integer> map;
    IAtomicLong operationCounter;
    IAtomicLong exceptionCounter;
    String basename;

    long globalKeyCount;
    int localKeyCount;

    void failOnVersionMismatch() {
        HazelcastTestUtils.failOnVersionMismatch("3.5", basename + ": This tests needs Hazelcast %s or newer");
    }

    void baseSetup(TestContext testContext, String basename) {
        this.hazelcastInstance = testContext.getTargetInstance();

        this.map = hazelcastInstance.getMap(basename);
        this.operationCounter = hazelcastInstance.getAtomicLong(basename + "Ops");
        this.exceptionCounter = hazelcastInstance.getAtomicLong(basename + "Exceptions");
        this.basename = basename;

        Integer minResultSizeLimit = 100000;
        Float resultLimitFactor = 1.15f;
        try {
            Class queryResultSizeLimiterClazz = getQueryResultSizeLimiterClass();
            minResultSizeLimit = getStaticFieldValue(queryResultSizeLimiterClazz, "MINIMUM_MAX_RESULT_LIMIT", int.class);
            resultLimitFactor = getStaticFieldValue(queryResultSizeLimiterClazz, "MAX_RESULT_LIMIT_FACTOR", float.class);
        } catch (Exception e) {
            LOGGER.warning(format("%s: QueryResultSizeLimiter is not implemented in this Hazelcast version", basename));
        }

        int clusterSize = hazelcastInstance.getCluster().getMembers().size();
        this.globalKeyCount = getGlobalKeyCount(minResultSizeLimit, resultLimitFactor);
        this.localKeyCount = (int) Math.ceil(globalKeyCount / (double) clusterSize);

        LOGGER.info(format("%s: Filling map with %d items (%d items per member, %d members in cluster)",
                basename, globalKeyCount, localKeyCount, clusterSize));
    }

    // due to class movement, the class can be at different locations depending on the hz version.
    static Class getQueryResultSizeLimiterClass() throws ClassNotFoundException {
        try {
            return Class.forName("com.hazelcast.map.impl.QueryResultSizeLimiter");
        } catch (ClassNotFoundException e) {
            return Class.forName("com.hazelcast.map.impl.query.QueryResultSizeLimiter");
        }
    }

    abstract long getGlobalKeyCount(Integer minResultSizeLimit, Float resultLimitFactor);

    void baseWarmup(String keyType) {
        if (!isMemberNode(hazelcastInstance)) {
            return;
        }

        int value = 0;
        Streamer<Object, Integer> streamer = StreamerFactory.getInstance(map);
        if ("String".equals(keyType)) {
            for (String key : generateStringKeys(localKeyCount, 10, KeyLocality.LOCAL, hazelcastInstance)) {
                streamer.pushEntry(key, value++);
            }
        } else if ("Integer".equals(keyType)) {
            for (int key : generateIntKeys(localKeyCount, Integer.MAX_VALUE, KeyLocality.LOCAL, hazelcastInstance)) {
                streamer.pushEntry(key, value++);
            }
        } else {
            throw new IllegalArgumentException("Unknown key type: " + keyType);
        }
        streamer.await();

        logPartitionStatistics(LOGGER, basename, map, true);
    }

    protected void baseVerify(boolean expectedExceptions) {
        int mapSize = map.size();
        long ops = operationCounter.get();
        long exceptions = exceptionCounter.get();

        LOGGER.info(basename + ": Map size: " + mapSize + ", Ops: " + ops + ", Exceptions: " + exceptions);

        assertTrue(format("Expected mapSize >= globalKeyCount (%d >= %d)", mapSize, globalKeyCount), mapSize >= globalKeyCount);
        assertTrue(format("Expected ops > 0 (%d > 0)", ops), ops > 0);
        if (expectedExceptions) {
            assertEquals("Expected as many exceptions as operations", ops, exceptions);
        } else {
            assertEquals("Expected 0 exceptions", 0, exceptions);
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
        protected void timeStep() throws Exception {
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
