package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.worker.performance.PerformanceState;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.FormatUtils.formatDouble;
import static com.hazelcast.simulator.utils.FormatUtils.formatLong;
import static com.hazelcast.simulator.utils.FormatUtils.formatPercentage;
import static com.hazelcast.simulator.worker.performance.PerformanceState.INTERVAL_LATENCY_PERCENTILE;
import static java.lang.String.format;

/**
 * Responsible for storing and formatting performance metrics from Simulator workers.
 */
public class PerformanceStateContainer {

    public static final String PERFORMANCE_FILE_NAME = "performance.txt";

    private static final int THROUGHPUT_FORMAT_LENGTH = 15;
    private static final int LATENCY_FORMAT_LENGTH = 10;

    private static final Logger LOGGER = Logger.getLogger(PerformanceStateContainer.class);

    private final ConcurrentMap<SimulatorAddress, Map<String, PerformanceState>> workerPerformanceStateMap
            = new ConcurrentHashMap<SimulatorAddress, Map<String, PerformanceState>>();

    public synchronized void updatePerformanceState(SimulatorAddress workerAddress,
                                                    Map<String, PerformanceState> performanceStates) {
        workerPerformanceStateMap.put(workerAddress, performanceStates);
    }

    String getPerformanceNumbers(String testCaseId) {
        PerformanceState performanceState = getPerformanceStateForTestCase(testCaseId);
        if (performanceState.isEmpty() || performanceState.getOperationCount() < 1) {
            return " (performance not available)";
        }
        return String.format("%s ops %s ops/s %s µs (%sth) %s µs (max)",
                formatLong(performanceState.getOperationCount(), THROUGHPUT_FORMAT_LENGTH),
                formatDouble(performanceState.getIntervalThroughput(), THROUGHPUT_FORMAT_LENGTH),
                formatLong(performanceState.getIntervalPercentileLatency(), LATENCY_FORMAT_LENGTH),
                INTERVAL_LATENCY_PERCENTILE,
                formatLong(performanceState.getIntervalMaxLatency(), LATENCY_FORMAT_LENGTH)
        );
    }

    synchronized PerformanceState getPerformanceStateForTestCase(String testCaseId) {
        PerformanceState performanceState = new PerformanceState();
        for (Map<String, PerformanceState> performanceStateMap : workerPerformanceStateMap.values()) {
            PerformanceState workerPerformanceState = performanceStateMap.get(testCaseId);
            if (workerPerformanceState != null) {
                performanceState.add(workerPerformanceState);
            }
        }
        return performanceState;
    }

    void logDetailedPerformanceInfo() {
        PerformanceState totalPerformanceState = new PerformanceState();
        Map<SimulatorAddress, PerformanceState> agentPerformanceStateMap = new HashMap<SimulatorAddress, PerformanceState>();

        calculatePerformanceStates(totalPerformanceState, agentPerformanceStateMap);

        long totalOperationCount = totalPerformanceState.getOperationCount();
        if (totalOperationCount < 1) {
            LOGGER.info("Performance information is not available!");
            return;
        }

        appendText(totalOperationCount + NEW_LINE, PERFORMANCE_FILE_NAME);
        LOGGER.info(format("Total performance       %s%% %s ops %s ops/s",
                formatPercentage(1, 1),
                formatLong(totalOperationCount, THROUGHPUT_FORMAT_LENGTH),
                formatDouble(totalPerformanceState.getTotalThroughput(), THROUGHPUT_FORMAT_LENGTH)));

        for (Map.Entry<SimulatorAddress, PerformanceState> entry : agentPerformanceStateMap.entrySet()) {
            SimulatorAddress agentAddress = entry.getKey();
            PerformanceState performanceState = entry.getValue();

            long operationCount = performanceState.getOperationCount();
            LOGGER.info(format("  Agent %-15s %s%% %s ops %s ops/s",
                    agentAddress,
                    formatPercentage(operationCount, totalOperationCount),
                    formatLong(operationCount, THROUGHPUT_FORMAT_LENGTH),
                    formatDouble(performanceState.getTotalThroughput(), THROUGHPUT_FORMAT_LENGTH)));
        }
    }

    synchronized void calculatePerformanceStates(PerformanceState totalPerformanceState,
                                                 Map<SimulatorAddress, PerformanceState> agentPerformanceStateMap) {
        for (Map.Entry<SimulatorAddress, Map<String, PerformanceState>> workerEntry : workerPerformanceStateMap.entrySet()) {
            SimulatorAddress agentAddress = workerEntry.getKey().getParent();

            PerformanceState agentPerformanceState = agentPerformanceStateMap.get(agentAddress);
            if (agentPerformanceState == null) {
                agentPerformanceState = new PerformanceState();
                agentPerformanceStateMap.put(agentAddress, agentPerformanceState);
            }

            for (PerformanceState performanceState : workerEntry.getValue().values()) {
                if (performanceState != null) {
                    totalPerformanceState.add(performanceState);
                    agentPerformanceState.add(performanceState);
                }
            }
        }
    }
}
