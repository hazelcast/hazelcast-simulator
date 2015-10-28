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
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.simulator.coordinator.CoordinatorUtils.logFailureInfo;
import static com.hazelcast.simulator.coordinator.CoordinatorUtils.waitForWorkerShutdown;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CoordinatorUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(CoordinatorUtils.class);
    }

    @Test(timeout = 10000)
    public void testWaitForWorkerShutdown() {
        final ConcurrentHashMap<SimulatorAddress, Boolean> finishedWorkers = new ConcurrentHashMap<SimulatorAddress, Boolean>();
        finishedWorkers.put(new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0), true);

        ThreadSpawner spawner = new ThreadSpawner("testWaitForFinishedWorker", true);
        spawner.spawn(new Runnable() {
            @Override
            public void run() {
                sleepSeconds(1);
                finishedWorkers.put(new SimulatorAddress(AddressLevel.WORKER, 1, 2, 0), true);
                sleepSeconds(1);
                finishedWorkers.put(new SimulatorAddress(AddressLevel.WORKER, 1, 3, 0), true);
            }
        });

        boolean success = waitForWorkerShutdown(3, finishedWorkers.keySet(), CoordinatorUtils.FINISHED_WORKER_TIMEOUT_SECONDS);
        assertTrue(success);
    }

    @Test(timeout = 10000)
    public void testWaitForWorkerShutdown_withTimeout() {
        ConcurrentHashMap<SimulatorAddress, Boolean> finishedWorkers = new ConcurrentHashMap<SimulatorAddress, Boolean>();
        finishedWorkers.put(new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0), true);

        boolean success = waitForWorkerShutdown(3, finishedWorkers.keySet(), 1);
        assertFalse(success);
    }

    @Test
    public void testLogFailureInfo_noFailures() {
        logFailureInfo(0);
    }

    @Test(expected = CommandLineExitException.class)
    public void testLogFailureInfo_withFailures() {
        logFailureInfo(1);
    }
}
