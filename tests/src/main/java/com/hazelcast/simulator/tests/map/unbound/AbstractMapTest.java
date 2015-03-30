/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.map.unbound;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;

abstract class AbstractMapTest {

    private static final ILogger LOGGER = Logger.getLogger(AbstractMapTest.class);

    HazelcastInstance hazelcastInstance;
    IMap<Object, Object> map;
    IAtomicLong operationCounter;
    IAtomicLong exceptionCounter;

    long globalKeyCount;
    int localKeyCount;

    void baseSetup(TestContext testContext, String basename) {
        hazelcastInstance = testContext.getTargetInstance();

        map = hazelcastInstance.getMap(basename);
        operationCounter = hazelcastInstance.getAtomicLong(basename + "Ops");
        exceptionCounter = hazelcastInstance.getAtomicLong(basename + "Exceptions");

        Class classType = null;
        try {
            classType = Class.forName("com.hazelcast.map.impl.MapQueryResultSizeLimitHelper");
        } catch (ClassNotFoundException e) {
            LOGGER.warning("Feature is not enabled in this version of Hazelcast!", e);
        }

        Integer minResultSizeLimit = 100000;
        Float resultLimitFactor = 1.15f;
        try {
            if (classType != null) {
                minResultSizeLimit = (Integer) classType.getDeclaredField("MINIMUM_MAX_RESULT_LIMIT").get(null);
                resultLimitFactor = (Float) classType.getDeclaredField("MAX_RESULT_LIMIT_FACTOR").get(null);
            }
        } catch (NoSuchFieldException e) {
            LOGGER.severe("Could not find expected field!", e);
        } catch (IllegalAccessException e) {
            LOGGER.severe("Could not read expected field!", e);
        }

        globalKeyCount = getGlobalKeyCount(minResultSizeLimit, resultLimitFactor);
        localKeyCount = 1 + (int) (globalKeyCount / hazelcastInstance.getCluster().getMembers().size());
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
        String[] keys = generateStringKeys(localKeyCount, 10, KeyLocality.Local, hazelcastInstance);
        int i = 0;
        for (String key : keys) {
            map.put(key, i++);
        }
    }

    void baseWarmupIntKey() {
        int[] keys = generateIntKeys(localKeyCount, Integer.MAX_VALUE, KeyLocality.Local, hazelcastInstance);
        int i = 0;
        for (int key : keys) {
            map.put(key, i++);
        }
    }

    AbstractWorker baseRunWithWorker(String operationType) {
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
        protected void afterRun() {
            operationCounter.addAndGet(localOperationCounter);
            exceptionCounter.addAndGet(localExceptionCounter);
        }
    }

    class ValuesWorker extends BaseWorker {

        @Override
        protected void timeStep() {
            localOperationCounter++;
            try {
                map.values();
            } catch (Exception e) {
                localExceptionCounter++;
            }
        }
    }

    class KeySetWorker extends BaseWorker {

        @Override
        protected void timeStep() {
            localOperationCounter++;
            try {
                map.keySet();
            } catch (Exception e) {
                localExceptionCounter++;
            }
        }
    }

    class EntrySetWorker extends BaseWorker {

        @Override
        protected void timeStep() {
            localOperationCounter++;
            try {
                map.entrySet();
            } catch (Exception e) {
                localExceptionCounter++;
            }
        }
    }
}
