package com.hazelcast.simulator.tests.external;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.probes.probes.SimpleProbe;
import com.hazelcast.simulator.probes.probes.impl.HdrLatencyDistributionProbe;
import com.hazelcast.simulator.probes.probes.impl.HdrLatencyDistributionResult;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static java.lang.String.format;

public class ExternalClientTest {

    private static final ILogger LOGGER = Logger.getLogger(ExternalClientTest.class);

    // properties
    public String basename = "externalClientsRunning";
    public boolean waitForClientsCountAutoDetection = true;
    public int waitForClientCountAutoDetectionDelaySeconds = 10;
    public int waitForClientsCount = 1;
    public int waitIntervalSeconds = 60;
    public int expectedResultSize = 0;

    SimpleProbe externalClientThroughput;
    IntervalProbe<HdrLatencyDistributionResult, HdrLatencyDistributionProbe> externalClientLatency;

    private TestContext testContext;
    private HazelcastInstance hazelcastInstance;
    private boolean isExternalResultsCollectorInstance;
    private ICountDownLatch clientsRunning;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        this.testContext = testContext;
        hazelcastInstance = testContext.getTargetInstance();

        if (isMemberNode(hazelcastInstance)) {
            return;
        }

        clientsRunning = hazelcastInstance.getCountDownLatch(basename);
        clientsRunning.trySetCount(waitForClientsCount);

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

        // lazy set CountdownLatch if client count auto detection is enabled
        if (waitForClientsCountAutoDetection) {
            // wait some seconds to be sure that all external clients are started
            LOGGER.info("Waiting for all external clients to be started...");
            sleepSeconds(waitForClientCountAutoDetectionDelaySeconds);
            waitForClientsCount = (int) hazelcastInstance.getAtomicLong("externalClientsStarted").get();
            clientsRunning.trySetCount(waitForClientsCount);
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
                LOGGER.info(format("Waiting for %d/%d clients...", clientsRunningCount, waitForClientsCount));
            } else {
                LOGGER.info(format("Got response from %d clients, stopping now!", waitForClientsCount));
                break;
            }
        }

        // just a single instance will collect the results from all external clients
        if (!isExternalResultsCollectorInstance) {
            // disable probes
            externalClientThroughput.disable();
            externalClientLatency.disable();

            LOGGER.info("Stopping non result collecting ExternalClientTest");
            testContext.stop();
            return;
        }

        // get probe results
        LOGGER.info("Collecting results from external clients...");
        getThroughputResults();
        getLatencyResults();
        LOGGER.info("Result collecting ExternalClientTest done!");

        testContext.stop();
    }

    private void getThroughputResults() {
        IList<String> throughputResults = getResultList("throughput", "externalClientsThroughputResults");
        int resultSize = throughputResults.size();

        LOGGER.info(format("Collecting %d throughput results (expected %d)...", resultSize, expectedResultSize));
        int totalInvocations = 0;
        double totalDuration = 0;
        for (String throughputString : throughputResults) {
            String[] throughput = throughputString.split("\\|");
            int operationCount = Integer.parseInt(throughput[0]);
            long duration = TimeUnit.NANOSECONDS.toMillis(Long.parseLong(throughput[1]));

            String publisherId = "n/a";
            if (throughput.length > 2) {
                publisherId = throughput[2];
            }
            LOGGER.info(format("External client executed %d operations in %d ms (%s)", operationCount, duration, publisherId));

            totalInvocations += operationCount;
            totalDuration += duration;
        }
        LOGGER.info("Done!");

        if (resultSize == 0 || totalInvocations == 0 || totalDuration == 0) {
            LOGGER.info(format("No valid throughput probe data collected! results: %d, totalInvocations: %d, totalDuration: %.0f",
                    resultSize, totalInvocations, totalDuration));
            return;
        }

        long avgDuration = Math.round(totalDuration / resultSize);
        externalClientThroughput.setValues(avgDuration, totalInvocations);
        double performance = ((double) totalInvocations / avgDuration) * 1000;
        LOGGER.info(format("All external clients executed %d operations in %d ms (%.3f ops/s)",
                totalInvocations, avgDuration, performance));
    }

    private void getLatencyResults() {
        IList<String> latencyLists = getResultList("latency", "externalClientsLatencyResults");

        LOGGER.info(format("Collecting %d latency result lists...", latencyLists.size()));
        for (String key : latencyLists) {
            IList<Long> values = hazelcastInstance.getList(key);
            LOGGER.info(format("Adding %d latency results...", values.size()));
            for (Long latency : values) {
                externalClientLatency.recordValue(latency);
            }
        }
        LOGGER.info("Done!");
    }

    private IList<String> getResultList(String typeName, String listName) {
        IList<String> resultList = hazelcastInstance.getList(listName);

        // wait for all throughput results to arrive
        int retries = 0;
        while (expectedResultSize > 0 && resultList.size() < expectedResultSize && retries++ < 60) {
            LOGGER.info(format("Waiting for %d/%d %s results...", resultList.size(), expectedResultSize, typeName));
            resultList = hazelcastInstance.getList(listName);
            sleepSeconds(1);
        }
        return resultList;
    }
}
