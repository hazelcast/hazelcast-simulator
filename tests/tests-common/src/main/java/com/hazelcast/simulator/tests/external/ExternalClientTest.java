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
package com.hazelcast.simulator.tests.external;

import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.tests.external.ExternalClientUtils.getLatencyResults;
import static com.hazelcast.simulator.tests.external.ExternalClientUtils.getThroughputResults;
import static com.hazelcast.simulator.tests.external.ExternalClientUtils.setCountDownLatch;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static java.lang.String.format;

public class ExternalClientTest extends AbstractTest {

    // properties
    public String name = "externalClientsRunning";
    public int waitForClientsCount = 0;
    public int waitIntervalSeconds = 60;
    public int expectedResultSize = 0;

    @InjectProbe
    private Probe externalClientProbe;

    private boolean isExternalResultsCollectorInstance;
    private ICountDownLatch clientsRunning;

    @Setup
    public void setUp() {
        if (isMemberNode(targetInstance)) {
            return;
        }

        // init ICountDownLatch with waitForClientsCount
        clientsRunning = targetInstance.getCountDownLatch(name);
        setCountDownLatch(clientsRunning, waitForClientsCount);

        // determine one instance per cluster
        if (targetInstance.getMap(name).putIfAbsent(name, true) == null) {
            isExternalResultsCollectorInstance = true;
            logger.info("This instance will collect all probe results from external clients");
        } else {
            logger.info("This instance will not collect probe results");
        }
    }

    @Run
    public void run() throws ExecutionException, InterruptedException {
        if (isMemberNode(targetInstance)) {
            return;
        }

        // wait for external clients to finish
        while (true) {
            try {
                clientsRunning.await(waitIntervalSeconds, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                EmptyStatement.ignore(ignored);
            }
            long clientsRunningCount = clientsRunning.getCount();
            if (clientsRunningCount > 0) {
                long responseReceivedCount = waitForClientsCount - clientsRunningCount;
                logger.info(format("Got response from %d/%d clients, waiting...", responseReceivedCount, waitForClientsCount));
            } else {
                logger.info(format("Got response from all %d clients, stopping now!", waitForClientsCount));
                break;
            }
        }

        // just a single instance will collect the results from all external clients
        if (!isExternalResultsCollectorInstance) {
            logger.info("Stopping non result collecting ExternalClientTest");
            return;
        }

        // get probe results
        logger.info("Collecting results from external clients...");
        getThroughputResults(targetInstance, expectedResultSize);
        getLatencyResults(targetInstance, externalClientProbe, expectedResultSize);
        logger.info("Result collecting ExternalClientTest done!");
    }
}
