package com.hazelcast.simulator.tests.external;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.tests.external.ExternalClientUtils.getLatencyResults;
import static com.hazelcast.simulator.tests.external.ExternalClientUtils.getThroughputResults;
import static com.hazelcast.simulator.tests.external.ExternalClientUtils.setCountDownLatch;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static java.lang.String.format;

public class ExternalClientTest {

    private static final ILogger LOGGER = Logger.getLogger(ExternalClientTest.class);

    // properties
    public String basename = "externalClientsRunning";
    public int waitForClientsCount = 0;
    public int waitIntervalSeconds = 60;
    public int expectedResultSize = 0;

    Probe externalClientProbe;

    private HazelcastInstance hazelcastInstance;
    private boolean isExternalResultsCollectorInstance;
    private ICountDownLatch clientsRunning;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        hazelcastInstance = testContext.getTargetInstance();

        if (isMemberNode(hazelcastInstance)) {
            return;
        }

        // init ICountDownLatch with waitForClientsCount
        clientsRunning = hazelcastInstance.getCountDownLatch(basename);
        setCountDownLatch(clientsRunning, waitForClientsCount);

        // determine one instance per cluster
        if (hazelcastInstance.getMap(basename).putIfAbsent(basename, true) == null) {
            isExternalResultsCollectorInstance = true;
            LOGGER.info("This instance will collect all probe results from external clients");
        } else {
            LOGGER.info("This instance will not collect probe results");
        }
    }

    @Run
    public void run() throws ExecutionException, InterruptedException {
        if (isMemberNode(hazelcastInstance)) {
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
                LOGGER.info(format("Got response from %d/%d clients, waiting...", responseReceivedCount, waitForClientsCount));
            } else {
                LOGGER.info(format("Got response from all %d clients, stopping now!", waitForClientsCount));
                break;
            }
        }

        // just a single instance will collect the results from all external clients
        if (!isExternalResultsCollectorInstance) {
            LOGGER.info("Stopping non result collecting ExternalClientTest");
            return;
        }

        // get probe results
        LOGGER.info("Collecting results from external clients...");
        getThroughputResults(hazelcastInstance, expectedResultSize);
        getLatencyResults(hazelcastInstance, externalClientProbe, expectedResultSize);
        LOGGER.info("Result collecting ExternalClientTest done!");
    }
}
