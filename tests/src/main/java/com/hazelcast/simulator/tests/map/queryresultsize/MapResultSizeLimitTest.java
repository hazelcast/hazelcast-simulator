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

import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.tasks.IWorker;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test verifies that {@link IMap#values()}, {@link IMap#keySet()} and {@link IMap#entrySet()} throw an exception if the
 * configured result size limit is exceeded.
 *
 * To achieve this we fill a map slightly above the trigger limit of the query result size limit, so we are sure it will always
 * trigger the exception. Then we call the map methods and verify that each call created an exception.
 *
 * The test can be configured to use {@link String} or {@link Integer} keys.
 *
 * @since Hazelcast 3.5
 */
public class MapResultSizeLimitTest extends AbstractMapTest {

    // properties
    public String basename = this.getClass().getSimpleName();
    public String keyType = "String";
    public String operationType = "values";

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        baseSetup(testContext, basename);

        failOnVersionMismatch(basename);
    }

    @Override
    long getGlobalKeyCount(Integer minResultSizeLimit, Float resultLimitFactor) {
        return Math.round(minResultSizeLimit * resultLimitFactor * 1.1);
    }

    @Warmup
    public void warmup() {
        baseWarmup(keyType);
    }

    @Verify(global = true)
    public void globalVerify() {
        if (!isMemberNode(hazelcastInstance)) {
            fail("We need a member worker to execute the global verify!");
            return;
        }

        int mapSize = map.size();
        assertTrue(format("Expected mapSize >= globalKeyCount (%d >= %d)", mapSize, globalKeyCount), mapSize >= globalKeyCount);

        long ops = operationCounter.get();
        long exceptions = exceptionCounter.get();
        assertEquals("Expected as many exceptions as operations", ops, exceptions);
    }

    @RunWithWorker
    public IWorker run() {
        return baseRunWithWorker(operationType);
    }

    public static void main(String[] args) throws Exception {
        new TestRunner<MapResultSizeLimitTest>(new MapResultSizeLimitTest()).run();
    }
}
