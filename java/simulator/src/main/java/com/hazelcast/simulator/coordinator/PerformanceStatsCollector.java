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

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.utils.FormatUtils.formatDouble;
import static com.hazelcast.simulator.utils.FormatUtils.formatLong;
import static com.hazelcast.simulator.utils.FormatUtils.formatPercentage;
import static com.hazelcast.simulator.utils.FormatUtils.secondsToHuman;
import static com.hazelcast.simulator.worker.performance.PerformanceStats.INTERVAL_LATENCY_PERCENTILE;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Responsible for storing and formatting performance metrics from Simulator workers.
 */
public class PerformanceStatsCollector {

    public static final int OPERATION_COUNT_FORMAT_LENGTH = 14;
    public static final int THROUGHPUT_FORMAT_LENGTH = 12;
    public static final int LATENCY_FORMAT_LENGTH = 10;

    private static final long DISPLAY_LATENCY_AS_MICROS_MAX_VALUE = MILLISECONDS.toMicros(10);

    // holds a map per Worker SimulatorAddress which contains the lastDelta PerformanceStats per testCaseId
    private final ConcurrentMap<SimulatorAddress, WorkerPerformance> workerPerformanceInfoMap
            = new ConcurrentHashMap<>();

    public void update(SimulatorAddress workerAddress, Map<String, PerformanceStats> performanceStatsMap) {
        WorkerPerformance workerPerformance = workerPerformanceInfoMap.get(workerAddress);
        if (workerPerformance == null) {
            WorkerPerformance newInfo = new WorkerPerformance();
            WorkerPerformance foundInfo = workerPerformanceInfoMap.putIfAbsent(workerAddress, newInfo);
            workerPerformance = foundInfo == null ? newInfo : foundInfo;
        }

        workerPerformance.updateAll(performanceStatsMap);
    }

    public String formatIntervalPerformanceNumbers(String testId) {
        PerformanceStats latest = get(testId, false);
        if (latest.isEmpty() || latest.getOperationCount() < 1) {
            return "";
        }

        double latencyAvgNs = latest.getIntervalLatencyAvgNanos();
        double latency999PercentileNs = latest.getIntervalLatency999PercentileNanos();
        double latencyMaxNs = latest.getIntervalLatencyMaxNanos();

        return format("%s ops %s ops/s %s %s (avg) %s %s (%sth) %s %s (max)",
                formatLong(latest.getOperationCount(), OPERATION_COUNT_FORMAT_LENGTH),
                formatDouble(latest.getIntervalThroughput(), THROUGHPUT_FORMAT_LENGTH),
                formatLong(toPrettyValue(latencyAvgNs), LATENCY_FORMAT_LENGTH),
                toPrettyUnit(latencyAvgNs),
                formatLong(toPrettyValue(latency999PercentileNs), LATENCY_FORMAT_LENGTH),
                toPrettyUnit(latency999PercentileNs),
                INTERVAL_LATENCY_PERCENTILE,
                formatLong(toPrettyValue(latencyMaxNs), LATENCY_FORMAT_LENGTH),
                toPrettyUnit(latencyMaxNs));
    }

    /**
     * If the valueNs is less than or equal to DISPLAY_LATENCY_AS_MICROS_MAX_VALUE,
     * it will return the time in microseconds and otherwise in nanoseconds.
     */
    private static long toPrettyValue(double valueNs) {
        long v = NANOSECONDS.toMicros(round(valueNs));
        if (v > DISPLAY_LATENCY_AS_MICROS_MAX_VALUE) {
            return MICROSECONDS.toMillis(v);
        } else {
            return v;
        }
    }

    /**
     * If the valueNs is less than or equal to DISPLAY_LATENCY_AS_MICROS_MAX_VALUE,
     * it will return "µs" and otherwise "ms".
     */
    private static String toPrettyUnit(double valueNs) {
        long value = NANOSECONDS.toMicros(round(valueNs));
        if (value > DISPLAY_LATENCY_AS_MICROS_MAX_VALUE) {
            return "ms";
        } else {
            return "µs";
        }
    }

    PerformanceStats get(String testCaseId, boolean aggregated) {
        // aggregate the PerformanceStats instances from all Workers by adding values (since from different Workers)
        PerformanceStats result = new PerformanceStats();

        for (WorkerPerformance workerPerformance : workerPerformanceInfoMap.values()) {
            PerformanceStats performanceStats = workerPerformance.get(testCaseId, aggregated);
            result.add(performanceStats);
        }

        return result;
    }

    public String detailedPerformanceInfo(String testId, long runningTimeMs) {
        PerformanceStats totalPerformanceStats = new PerformanceStats();
        Map<SimulatorAddress, PerformanceStats> agentPerformanceStatsMap = new HashMap<>();
        calculatePerformanceStats(testId, totalPerformanceStats, agentPerformanceStatsMap);

        long totalOperationCount = totalPerformanceStats.getOperationCount();

        if (totalOperationCount < 1) {
            return "Performance information is not available!";
        }

        double runningTimeSeconds = (runningTimeMs * 1d) / SECONDS.toMillis(1);

        StringBuilder sb = new StringBuilder();

        double throughput = totalOperationCount / runningTimeSeconds;
        sb.append("Total running time " + secondsToHuman(Math.round(runningTimeSeconds)) + "\n");

        sb.append(format("Total throughput        %s%% %s ops %s ops/s\n",
                formatPercentage(1, 1),
                formatLong(totalOperationCount, OPERATION_COUNT_FORMAT_LENGTH),
                formatDouble(throughput, THROUGHPUT_FORMAT_LENGTH)));


        for (SimulatorAddress address : sort(agentPerformanceStatsMap.keySet())) {
            PerformanceStats performanceStats = agentPerformanceStatsMap.get(address);

            long operationCount = performanceStats.getOperationCount();
            sb.append(format("  Agent %-15s %s%% %s ops %s ops/s\n",
                    address,
                    formatPercentage(operationCount, totalOperationCount),
                    formatLong(operationCount, OPERATION_COUNT_FORMAT_LENGTH),
                    formatDouble(operationCount / runningTimeSeconds, THROUGHPUT_FORMAT_LENGTH)));
        }
        return sb.toString();
    }

    void calculatePerformanceStats(String testId,
                                   PerformanceStats totalPerformanceStats,
                                   Map<SimulatorAddress, PerformanceStats> agentPerformanceStatsMap) {

        for (Map.Entry<SimulatorAddress, WorkerPerformance> entry : workerPerformanceInfoMap.entrySet()) {
            SimulatorAddress workerAddress = entry.getKey();
            SimulatorAddress agentAddress = workerAddress.getParent();
            PerformanceStats agentPerformanceStats = agentPerformanceStatsMap.get(agentAddress);
            if (agentPerformanceStats == null) {
                agentPerformanceStats = new PerformanceStats();
                agentPerformanceStatsMap.put(agentAddress, agentPerformanceStats);
            }

            WorkerPerformance workerPerformance = entry.getValue();
            PerformanceStats workerTestPerformanceStats = workerPerformance.get(testId, true);
            if (workerTestPerformanceStats != null) {
                agentPerformanceStats.add(workerTestPerformanceStats);
                totalPerformanceStats.add(workerTestPerformanceStats);
            }
        }
    }

    private List<SimulatorAddress> sort(Set<SimulatorAddress> addresses) {
        List<SimulatorAddress> list = new LinkedList<>(addresses);
        list.sort(Comparator.comparing(SimulatorAddress::toString));
        return list;
    }

    /**
     * Contains the performance info for a given worker.
     */
    private final class WorkerPerformance {
        // contains the performance per test. Key is test-id.
        private final ConcurrentMap<String, TestPerformance> testPerformanceMap
                = new ConcurrentHashMap<>();

        private void updateAll(Map<String, PerformanceStats> deltas) {
            for (Map.Entry<String, PerformanceStats> entry : deltas.entrySet()) {
                update(entry.getKey(), entry.getValue());
            }
        }

        private void update(String testId, PerformanceStats delta) {
            for (; ; ) {
                TestPerformance current = testPerformanceMap.get(testId);
                if (current == null) {
                    if (testPerformanceMap.putIfAbsent(testId, new TestPerformance(delta, delta)) == null) {
                        return;
                    }
                } else {
                    TestPerformance update = current.update(delta);
                    if (testPerformanceMap.replace(testId, current, update)) {
                        return;
                    }
                }
            }
        }

        private PerformanceStats get(String testId, boolean aggregated) {
            TestPerformance testPerformance = testPerformanceMap.get(testId);
            if (testPerformance == null) {
                return new PerformanceStats();
            }
            return aggregated ? testPerformance.aggregated : testPerformance.lastDelta;
        }
    }

    /**
     * Contains the latest and aggregated performance info.
     */
    private final class TestPerformance {
        private final PerformanceStats aggregated;
        private final PerformanceStats lastDelta;

        private TestPerformance(PerformanceStats aggregated, PerformanceStats lastDelta) {
            this.aggregated = aggregated;
            this.lastDelta = lastDelta;
        }

        private TestPerformance update(PerformanceStats delta) {
            PerformanceStats newAggregated = new PerformanceStats(aggregated);
            newAggregated.add(delta, false);
            return new TestPerformance(newAggregated, delta);
        }
    }
}
