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

import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test creates latency probe results for {@link IMap#values()}, {@link IMap#keySet()} and {@link IMap#entrySet()}. It is
 * used to ensure that the "Unbounded return values" feature has no bad impact on the latency of those method calls. The test
 * can be configured to use {@link String} or {@link Integer} keys.
 */
public class MapLatencyTest extends AbstractMapTest {

    // properties
    public String basename = this.getClass().getSimpleName();
    public String keyType = "String";
    public String operationType = "values";

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        baseSetup(testContext, basename);
    }

    @Override
    long getGlobalKeyCount(Integer minResultSizeLimit, Float resultLimitFactor) {
        return Math.round(minResultSizeLimit * resultLimitFactor * 0.9);
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
        assertTrue(format("Expected ops > 0 (%d > 0)", ops), ops > 0);

        assertEquals("Expected 0 exceptions", 0, exceptionCounter.get());
    }

    @RunWithWorker
    public AbstractWorker run() {
        return baseRunWithWorker(operationType);
    }

    public static void main(String[] args) throws Throwable {
        new TestRunner<MapLatencyTest>(new MapLatencyTest()).run();
    }
}
