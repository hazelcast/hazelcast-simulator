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
import com.hazelcast.simulator.worker.performance.PerformanceStats;
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

import static com.hazelcast.simulator.utils.FormatUtils.formatDouble;
import static com.hazelcast.simulator.utils.FormatUtils.formatLong;
import static com.hazelcast.simulator.utils.FormatUtils.formatPercentage;
import static com.hazelcast.simulator.worker.performance.PerformanceStats.INTERVAL_LATENCY_PERCENTILE;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Responsible for storing and formatting performance metrics from Simulator workers.
 */
public class PerformanceStatsCollector {

    public static final int OPERATION_COUNT_FORMAT_LENGTH = 14;
    public static final int THROUGHPUT_FORMAT_LENGTH = 12;
    public static final int LATENCY_FORMAT_LENGTH = 10;

    private static final long DISPLAY_LATENCY_AS_MICROS_MAX_VALUE = TimeUnit.SECONDS.toMicros(1);

    private static final Logger LOGGER = Logger.getLogger(PerformanceStatsCollector.class);

    // holds a map per Worker SimulatorAddress which contains the last PerformanceStats per testCaseId
    private final ConcurrentMap<SimulatorAddress, ConcurrentMap<String, PerformanceStats>> workerLastPerformanceStatsMap
            = new ConcurrentHashMap<SimulatorAddress, ConcurrentMap<String, PerformanceStats>>();

    // holds a queue per test with pending PerformanceStats messages. The key is the testId.
    private final ConcurrentMap<String, Queue<WorkerPerformanceStats>> pendingQueueByTestMap
            = new ConcurrentHashMap<String, Queue<WorkerPerformanceStats>>();

    public void update(SimulatorAddress workerAddress, Map<String, PerformanceStats> performanceStatsMap) {
        for (Map.Entry<String, PerformanceStats> entry : performanceStatsMap.entrySet()) {
            String testCaseId = entry.getKey();
            PerformanceStats performanceStats = entry.getValue();

            ConcurrentMap<String, PerformanceStats> lastPerformanceStatsMap = getOrCreateLastPerformanceStatsMap(workerAddress);
            lastPerformanceStatsMap.put(testCaseId, performanceStats);

            Queue<WorkerPerformanceStats> pendingQueue = pendingQueueByTestMap.get(testCaseId);
            if (pendingQueue == null) {
                Queue<WorkerPerformanceStats> newQueue = new ConcurrentLinkedQueue<WorkerPerformanceStats>();
                Queue<WorkerPerformanceStats> foundQueue = pendingQueueByTestMap.putIfAbsent(testCaseId, newQueue);
                pendingQueue = foundQueue == null ? newQueue : foundQueue;
            }

            pendingQueue.add(new WorkerPerformanceStats(workerAddress, performanceStats));
        }
    }

    public String formatPerformanceNumbers(String testCaseId) {
        PerformanceStats performanceStats = get(testCaseId);
        if (performanceStats.isEmpty() || performanceStats.getOperationCount() < 1) {
            return "";
        }
        String latencyUnit = "µs";
        long latencyAvg = NANOSECONDS.toMicros(round(performanceStats.getIntervalLatencyAvgNanos()));
        long latency999Percentile = NANOSECONDS.toMicros(performanceStats.getIntervalLatency999PercentileNanos());
        long latencyMax = NANOSECONDS.toMicros(performanceStats.getIntervalLatencyMaxNanos());

        if (latencyAvg > DISPLAY_LATENCY_AS_MICROS_MAX_VALUE) {
            latencyUnit = "ms";
            latencyAvg = MICROSECONDS.toMillis(latencyAvg);
            latency999Percentile = MICROSECONDS.toMillis(latency999Percentile);
            latencyMax = MICROSECONDS.toMillis(latencyMax);
        }

        return String.format("%s ops %s ops/s %s %s (avg) %s %s (%sth) %s %s (max)",
                formatLong(performanceStats.getOperationCount(), OPERATION_COUNT_FORMAT_LENGTH),
                formatDouble(performanceStats.getIntervalThroughput(), THROUGHPUT_FORMAT_LENGTH),
                formatLong(latencyAvg, LATENCY_FORMAT_LENGTH),
                latencyUnit,
                formatLong(latency999Percentile, LATENCY_FORMAT_LENGTH),
                latencyUnit,
                INTERVAL_LATENCY_PERCENTILE,
                formatLong(latencyMax, LATENCY_FORMAT_LENGTH),
                latencyUnit
        );
    }

    PerformanceStats get(String testCaseId) {
        // return if no queue of WorkerPerformanceStats can be found (unknown testCaseId)
        Queue<WorkerPerformanceStats> pendingQueue = pendingQueueByTestMap.get(testCaseId);
        if (pendingQueue == null) {
            return new PerformanceStats();
        }

        // aggregate the PerformanceStats instances per Worker by maximum values (since from same Worker)
        Map<SimulatorAddress, PerformanceStats> workerPerformanceStatsMap = new HashMap<SimulatorAddress, PerformanceStats>();
        for (; ; ) {
            WorkerPerformanceStats pending = pendingQueue.poll();
            if (pending == null) {
                // we have drained the queue of pending work, so we are ready
                break;
            }

            PerformanceStats candidate = workerPerformanceStatsMap.get(pending.simulatorAddress);
            if (candidate == null) {
                workerPerformanceStatsMap.put(pending.simulatorAddress, pending.performanceStats);
            } else {
                candidate.add(pending.performanceStats, false);
            }
        }

        // aggregate the PerformanceStats instances from all Workers by adding values (since from different Workers)
        PerformanceStats result = new PerformanceStats();
        for (PerformanceStats workerPerformanceStats : workerPerformanceStatsMap.values()) {
            result.add(workerPerformanceStats);
        }
        return result;
    }

    void logDetailedPerformanceInfo(double runningTimeSeconds) {
        PerformanceStats totalPerformanceStats = new PerformanceStats();
        Map<SimulatorAddress, PerformanceStats> agentPerformanceStatsMap = new HashMap<SimulatorAddress, PerformanceStats>();

        calculatePerformanceStats(totalPerformanceStats, agentPerformanceStatsMap);

        long totalOperationCount = totalPerformanceStats.getOperationCount();
        if (totalOperationCount < 1) {
            LOGGER.info("Performance information is not available!");
            return;
        }
        LOGGER.info(format("Total throughput        %s%% %s ops %s ops/s",
                formatPercentage(1, 1),
                formatLong(totalOperationCount, OPERATION_COUNT_FORMAT_LENGTH),
                formatDouble(totalOperationCount / runningTimeSeconds, THROUGHPUT_FORMAT_LENGTH)));

        for (SimulatorAddress address : sort(agentPerformanceStatsMap.keySet())) {
            PerformanceStats performanceStats = agentPerformanceStatsMap.get(address);

            long operationCount = performanceStats.getOperationCount();
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

    void calculatePerformanceStats(PerformanceStats totalPerformanceStats,
                                   Map<SimulatorAddress, PerformanceStats> agentPerformanceStatsMap) {
        for (Map.Entry<SimulatorAddress, ConcurrentMap<String, PerformanceStats>> workerEntry
                : workerLastPerformanceStatsMap.entrySet()) {
            SimulatorAddress agentAddress = workerEntry.getKey().getParent();

            // get or create PerformanceStats for agentAddress
            PerformanceStats agentPerformanceStats = agentPerformanceStatsMap.get(agentAddress);
            if (agentPerformanceStats == null) {
                agentPerformanceStats = new PerformanceStats();
                agentPerformanceStatsMap.put(agentAddress, agentPerformanceStats);
            }

            // aggregate the PerformanceStats instances per Agent and in total
            for (PerformanceStats performanceStats : workerEntry.getValue().values()) {
                if (performanceStats != null) {
                    totalPerformanceStats.add(performanceStats);
                    agentPerformanceStats.add(performanceStats);
                }
            }
        }
    }

    private ConcurrentMap<String, PerformanceStats> getOrCreateLastPerformanceStatsMap(SimulatorAddress workerAddress) {
        ConcurrentMap<String, PerformanceStats> map = workerLastPerformanceStatsMap.get(workerAddress);
        if (map != null) {
            return map;
        }

        map = new ConcurrentHashMap<String, PerformanceStats>();
        ConcurrentMap<String, PerformanceStats> found = workerLastPerformanceStatsMap.putIfAbsent(workerAddress, map);
        return found == null ? map : found;
    }

    private static final class WorkerPerformanceStats {

        private final SimulatorAddress simulatorAddress;
        private final PerformanceStats performanceStats;

        WorkerPerformanceStats(SimulatorAddress simulatorAddress, PerformanceStats performanceStats) {
            this.simulatorAddress = simulatorAddress;
            this.performanceStats = performanceStats;
        }
    }
}
