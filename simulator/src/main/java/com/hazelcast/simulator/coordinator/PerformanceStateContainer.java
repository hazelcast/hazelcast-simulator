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
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.worker.performance.PerformanceState;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.FormatUtils.formatDouble;
import static com.hazelcast.simulator.utils.FormatUtils.formatLong;
import static com.hazelcast.simulator.utils.FormatUtils.formatPercentage;
import static com.hazelcast.simulator.worker.performance.PerformanceState.INTERVAL_LATENCY_PERCENTILE;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Responsible for storing and formatting performance metrics from Simulator workers.
 */
public class PerformanceStateContainer {

    public static final int OPERATION_COUNT_FORMAT_LENGTH = 14;
    public static final int THROUGHPUT_FORMAT_LENGTH = 12;
    public static final int LATENCY_FORMAT_LENGTH = 10;

    static final String PERFORMANCE_FILE_NAME = "performance.txt";

    private static final long DISPLAY_LATENCY_AS_MICROS_MAX_VALUE = TimeUnit.SECONDS.toMicros(1);

    private static final Logger LOGGER = Logger.getLogger(PerformanceStateContainer.class);

    // holds a map per Worker SimulatorAddress which contains the last PerformanceState per testCaseId
    private final ConcurrentMap<SimulatorAddress, ConcurrentMap<String, PerformanceState>> workerLastPerformanceStateMap
            = new ConcurrentHashMap<SimulatorAddress, ConcurrentMap<String, PerformanceState>>();

    // holds a queue per test with pending PerformanceState messages. The key is the testId.
    private final ConcurrentMap<String, Queue<WorkerPerformanceState>> pendingQueueByTestMap
            = new ConcurrentHashMap<String, Queue<WorkerPerformanceState>>();

    public void update(SimulatorAddress workerAddress, Map<String, PerformanceState> performanceStates) {
        for (Map.Entry<String, PerformanceState> entry : performanceStates.entrySet()) {
            String testCaseId = entry.getKey();
            PerformanceState performanceState = entry.getValue();

            ConcurrentMap<String, PerformanceState> lastPerformanceStateMap = getOrCreateLastPerformanceStateMap(workerAddress);
            lastPerformanceStateMap.put(testCaseId, performanceState);

            Queue<WorkerPerformanceState> pendingQueue = pendingQueueByTestMap.get(testCaseId);
            if (pendingQueue == null) {
                Queue<WorkerPerformanceState> newQueue = new ConcurrentLinkedQueue<WorkerPerformanceState>();
                Queue<WorkerPerformanceState> foundQueue = pendingQueueByTestMap.putIfAbsent(testCaseId, newQueue);
                pendingQueue = foundQueue == null ? newQueue : foundQueue;
            }

            pendingQueue.add(new WorkerPerformanceState(workerAddress, performanceState));
        }
    }

    public String formatPerformanceNumbers(String testCaseId) {
        PerformanceState performanceState = get(testCaseId);
        if (performanceState.isEmpty() || performanceState.getOperationCount() < 1) {
            return "";
        }
        String latencyUnit = "µs";
        long avgLatencyValue = round(performanceState.getIntervalAvgLatency());
        long percentileLatencyValue = performanceState.getIntervalPercentileLatency();
        long maxLatencyValue = performanceState.getIntervalMaxLatency();
        if (avgLatencyValue > DISPLAY_LATENCY_AS_MICROS_MAX_VALUE) {
            latencyUnit = "ms";
            avgLatencyValue = MICROSECONDS.toMillis(avgLatencyValue);
            percentileLatencyValue = MICROSECONDS.toMillis(percentileLatencyValue);
            maxLatencyValue = MICROSECONDS.toMillis(maxLatencyValue);
        }
        return String.format("%s ops %s ops/s %s %s (avg) %s %s (%sth) %s %s (max)",
                formatLong(performanceState.getOperationCount(), OPERATION_COUNT_FORMAT_LENGTH),
                formatDouble(performanceState.getIntervalThroughput(), THROUGHPUT_FORMAT_LENGTH),
                formatLong(avgLatencyValue, LATENCY_FORMAT_LENGTH),
                latencyUnit,
                formatLong(percentileLatencyValue, LATENCY_FORMAT_LENGTH),
                latencyUnit,
                INTERVAL_LATENCY_PERCENTILE,
                formatLong(maxLatencyValue, LATENCY_FORMAT_LENGTH),
                latencyUnit
        );
    }

    PerformanceState get(String testCaseId) {
        // return if no queue of WorkerPerformanceState can be found (unknown testCaseId)
        Queue<WorkerPerformanceState> pendingQueue = pendingQueueByTestMap.get(testCaseId);
        if (pendingQueue == null) {
            return new PerformanceState();
        }

        // aggregate the PerformanceState instances per Worker by maximum values (since from same Worker)
        Map<SimulatorAddress, PerformanceState> workerPerformanceStateMap = new HashMap<SimulatorAddress, PerformanceState>();
        for (; ; ) {
            WorkerPerformanceState pending = pendingQueue.poll();
            if (pending == null) {
                // we have drained the queue of pending work, so we are ready
                break;
            }

            PerformanceState candidate = workerPerformanceStateMap.get(pending.simulatorAddress);
            if (candidate == null) {
                workerPerformanceStateMap.put(pending.simulatorAddress, pending.performanceState);
            } else {
                candidate.add(pending.performanceState, false);
            }
        }

        // aggregate the PerformanceState instances from all Workers by adding values (since from different Workers)
        PerformanceState result = new PerformanceState();
        for (PerformanceState workerPerformanceState : workerPerformanceStateMap.values()) {
            result.add(workerPerformanceState);
        }
        return result;
    }

    void logDetailedPerformanceInfo(double runningTimeSeconds) {
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
                formatLong(totalOperationCount, OPERATION_COUNT_FORMAT_LENGTH),
                formatDouble(totalOperationCount / runningTimeSeconds, THROUGHPUT_FORMAT_LENGTH)));


        for (SimulatorAddress address : sort(agentPerformanceStateMap.keySet())) {
            PerformanceState performanceState = agentPerformanceStateMap.get(address);

            long operationCount = performanceState.getOperationCount();
            LOGGER.info(format("  Agent %-15s %s%% %s ops %s ops/s",
                    address,
                    formatPercentage(operationCount, totalOperationCount),
                    formatLong(operationCount, OPERATION_COUNT_FORMAT_LENGTH),
                    formatDouble(operationCount / runningTimeSeconds, THROUGHPUT_FORMAT_LENGTH)));
        }
    }

    private List<SimulatorAddress> sort(Set<SimulatorAddress> addresses) {
        List<SimulatorAddress> list = new LinkedList<SimulatorAddress>(addresses);
        Collections.sort(list, new Comparator<SimulatorAddress>() {
            @Override
            public int compare(SimulatorAddress o1, SimulatorAddress o2) {
                return o1.toString().compareTo(o2.toString());
            }
        });
        return list;
    }

    void calculatePerformanceStates(PerformanceState totalPerformanceState,
                                    Map<SimulatorAddress, PerformanceState> agentPerformanceStateMap) {
        for (Map.Entry<SimulatorAddress, ConcurrentMap<String, PerformanceState>> workerEntry
                : workerLastPerformanceStateMap.entrySet()) {
            SimulatorAddress agentAddress = workerEntry.getKey().getParent();

            // get or create PerformanceState for agentAddress
            PerformanceState agentPerformanceState = agentPerformanceStateMap.get(agentAddress);
            if (agentPerformanceState == null) {
                agentPerformanceState = new PerformanceState();
                agentPerformanceStateMap.put(agentAddress, agentPerformanceState);
            }

            // aggregate the PerformanceState instances per Agent and in total
            for (PerformanceState performanceState : workerEntry.getValue().values()) {
                if (performanceState != null) {
                    totalPerformanceState.add(performanceState);
                    agentPerformanceState.add(performanceState);
                }
            }
        }
    }

    private ConcurrentMap<String, PerformanceState> getOrCreateLastPerformanceStateMap(SimulatorAddress workerAddress) {
        ConcurrentMap<String, PerformanceState> map = workerLastPerformanceStateMap.get(workerAddress);
        if (map != null) {
            return map;
        }

        map = new ConcurrentHashMap<String, PerformanceState>();
        ConcurrentMap<String, PerformanceState> found = workerLastPerformanceStateMap.putIfAbsent(workerAddress, map);
        return found == null ? map : found;
    }

    private static final class WorkerPerformanceState {

        private final SimulatorAddress simulatorAddress;
        private final PerformanceState performanceState;

        WorkerPerformanceState(SimulatorAddress simulatorAddress, PerformanceState performanceState) {
            this.simulatorAddress = simulatorAddress;
            this.performanceState = performanceState;
        }
    }
}
