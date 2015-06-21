package com.hazelcast.simulator.tests.external;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.core.IList;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.probes.probes.SimpleProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class ExternalClientTest {

    private static final ILogger LOGGER = Logger.getLogger(ExternalClientTest.class);

    // properties
    public String basename = "externalClientsRunning";
    public int waitForClientsCount = 1;
    public int waitIntervalSeconds = 60;

    IntervalProbe externalClientLatency;
    SimpleProbe externalClientThroughput;

    private TestContext testContext;
    private HazelcastInstance hazelcastInstance;
    private ICountDownLatch clientsRunning;
    private boolean isExternalResultsCollectorInstance;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        this.testContext = testContext;
        hazelcastInstance = testContext.getTargetInstance();

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
    public void run() {
        // wait for external clients to finish
        while (true) {
            try {
                clientsRunning.await(waitIntervalSeconds, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                EmptyStatement.ignore(ignored);
            }
            long clientsRunningCount = clientsRunning.getCount();
            if (clientsRunningCount > 0) {
                LOGGER.info("Waiting for " + clientsRunningCount + " clients...");
            } else {
                LOGGER.info("Got response from " + waitForClientsCount + " clients, stopping now!");
                break;
            }
        }

        // just a single instance will collect the results from all external clients
        if (!isExternalResultsCollectorInstance) {
            // disable probes
            externalClientLatency.disable();
            externalClientThroughput.disable();

            LOGGER.info("Stopping non result collecting ExternalClientTest");
            testContext.stop();
            return;
        }
        LOGGER.info("Collecting results from external clients...");

        // fetch latency results
        IList<Long> latencyResults = hazelcastInstance.getList("externalClientsLatencyResults");
        int latencyResultSize = latencyResults.size();

        LOGGER.info(format("Collecting %d latency results...", latencyResultSize));
        Long[] latencyArray = new Long[latencyResultSize];
        latencyResults.<Long>toArray(latencyArray);

        LOGGER.info(format("Adding %d latency results to probe...", latencyResultSize));
        int counter = 0;
        for (Long latency : latencyArray) {
            externalClientLatency.recordValue(latency);
            if (++counter % 100000 == 0) {
                LOGGER.info(format("Collected %d/%d latency results...", counter, latencyResultSize));
            }
        }
        LOGGER.info("Done!");

        // fetch throughput results
        IList<String> throughputResults = hazelcastInstance.getList("externalClientsThroughputResults");
        LOGGER.info("Collecting " + throughputResults.size() + " throughput results...");

        double totalDuration = 0;
        int totalInvocations = 0;
        for (String throughputString : throughputResults) {
            String[] throughput = throughputString.split("\\|");
            long operationCount = Long.valueOf(throughput[0]);
            long duration = TimeUnit.NANOSECONDS.toMillis(Long.valueOf(throughput[1]));

            LOGGER.info(format("External client executed %d operations in %d ms", operationCount, duration));

            totalDuration += duration;
            totalInvocations += operationCount;
        }
        LOGGER.info("Done!");

        long durationAvg = Math.round(totalDuration / throughputResults.size());
        LOGGER.info(format("All external clients executed %d operations in %d ms", totalInvocations, durationAvg));
        externalClientThroughput.setValues(durationAvg, totalInvocations);

        LOGGER.info("Stopping result collecting ExternalClientTest");
        testContext.stop();
    }
}
