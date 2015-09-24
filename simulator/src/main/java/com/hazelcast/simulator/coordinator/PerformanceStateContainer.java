package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.CommonUtils;
import com.hazelcast.simulator.worker.performance.PerformanceState;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.formatDouble;
import static com.hazelcast.simulator.utils.CommonUtils.formatLong;
import static com.hazelcast.simulator.utils.CommonUtils.formatPercentage;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.worker.performance.PerformanceState.EMPTY_OPERATION_COUNT;
import static java.lang.String.format;

/**
 * Responsible for storing and formatting performance metrics from Simulator workers.
 */
public class PerformanceStateContainer {

    public static final String PERFORMANCE_FILE_NAME = "performance.txt";

    private static final int NUMBER_FORMAT_LENGTH = 15;

    private static final Logger LOGGER = Logger.getLogger(PerformanceStateContainer.class);

    private final AtomicBoolean performanceWritten = new AtomicBoolean();
    private final ConcurrentMap<SimulatorAddress, Map<String, PerformanceState>> performanceStatePerWorker
            = new ConcurrentHashMap<SimulatorAddress, Map<String, PerformanceState>>();

    public synchronized void updatePerformanceState(SimulatorAddress workerAddress,
                                       Map<String, PerformanceState> performanceStates) {
        performanceStatePerWorker.put(workerAddress, performanceStates);
    }

    String getPerformanceNumbers(String testCaseId) {
        PerformanceState performanceState = getPerformanceStateForTestCase(testCaseId);
        if (performanceState.getOperationCount() == EMPTY_OPERATION_COUNT) {
            return " (performance not available)";
        }
        return String.format("%s ops %s ops/s",
                formatLong(performanceState.getOperationCount(), NUMBER_FORMAT_LENGTH),
                formatDouble(performanceState.getIntervalThroughput(), NUMBER_FORMAT_LENGTH)
        );
    }

    synchronized PerformanceState getPerformanceStateForTestCase(String testCaseId) {
        PerformanceState performanceState = new PerformanceState();
        for (Map<String, PerformanceState> performanceStateMap : performanceStatePerWorker.values()) {
            PerformanceState workerPerformanceState = performanceStateMap.get(testCaseId);
            if (workerPerformanceState != null) {
                performanceState.add(workerPerformanceState);
            }
        }
        return performanceState;
    }

    void logDetailedPerformanceInfo() {
        if (!performanceWritten.compareAndSet(false, true)) {
            return;
        }

        Map<SimulatorAddress, PerformanceState> performancePerAgent = new HashMap<SimulatorAddress, PerformanceState>();
        PerformanceState totalPerformanceState = new PerformanceState();

        calculatePerformanceStates(performancePerAgent, totalPerformanceState);

        long totalOperationCount = totalPerformanceState.getOperationCount();
        if (totalOperationCount == EMPTY_OPERATION_COUNT) {
            LOGGER.info("Performance information is not available!");
            return;
        }

        appendText(totalOperationCount + CommonUtils.NEW_LINE, PERFORMANCE_FILE_NAME);
        if (totalOperationCount > 0) {
            LOGGER.info(format("Total performance       %s%% %s ops %s ops/s",
                    formatPercentage(1, 1),
                    formatLong(totalOperationCount, NUMBER_FORMAT_LENGTH),
                    formatDouble(totalPerformanceState.getTotalThroughput(), NUMBER_FORMAT_LENGTH)));
        }

        for (Map.Entry<SimulatorAddress, PerformanceState> entry : performancePerAgent.entrySet()) {
            SimulatorAddress agentAddress = entry.getKey();
            PerformanceState performanceState = entry.getValue();

            long operationCount = performanceState.getOperationCount();
            LOGGER.info(format("  Agent %-15s %s%% %s ops %s ops/s",
                    agentAddress,
                    formatPercentage(operationCount, totalOperationCount),
                    formatLong(operationCount, NUMBER_FORMAT_LENGTH),
                    formatDouble(performanceState.getTotalThroughput(), NUMBER_FORMAT_LENGTH)));
        }
    }

    synchronized void calculatePerformanceStates(Map<SimulatorAddress, PerformanceState> performancePerAgent,
                                    PerformanceState totalPerformanceState) {
        for (Map.Entry<SimulatorAddress, Map<String, PerformanceState>> workerEntry : performanceStatePerWorker.entrySet()) {
            SimulatorAddress agentAddress = workerEntry.getKey().getParent();

            PerformanceState agentPerformanceState = performancePerAgent.get(agentAddress);
            if (agentPerformanceState == null) {
                agentPerformanceState = new PerformanceState();
                performancePerAgent.put(agentAddress, agentPerformanceState);
            }

            for (PerformanceState performanceState : workerEntry.getValue().values()) {
                if (performanceState != null) {
                    agentPerformanceState.add(performanceState);
                    totalPerformanceState.add(performanceState);
                }
            }
        }
    }
}
