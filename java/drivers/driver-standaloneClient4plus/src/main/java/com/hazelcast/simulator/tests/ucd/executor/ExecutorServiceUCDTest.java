/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.ucd.executor;

import com.hazelcast.core.IExecutorService;
import com.hazelcast.simulator.probes.LatencyProbe;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.ucd.UCDTest;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ExecutorServiceUCDTest extends UCDTest {
    private IExecutorService executor;

    @Override
    @Setup
    public void setUp() throws ReflectiveOperationException  {
        executor = targetInstance.getExecutorService(name);
        super.setUp();
        executor.submit((Callable<Long>) udf.getDeclaredConstructor(long.class)
                .newInstance(System.nanoTime()));
    }

    @TimeStep
    public void timeStep(LatencyProbe latencyProbe) throws Exception {
        Future<Long> future = executor.submit((Callable<Long>) udf.getDeclaredConstructor(long.class)
                .newInstance(System.nanoTime()));

        long start = future.get();
        latencyProbe.done(start);
    }

    @Teardown(global = true)
    public void teardown() throws InterruptedException {
        executor.shutdownNow();
        if (!executor.awaitTermination(120, TimeUnit.SECONDS)) {
            logger.fatal("Time out while waiting for shutdown of executor: {}", executor.getName());
        }
        executor.destroy();
    }
}
