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
import com.hazelcast.simulator.worker.performance.IntervalStats;

import java.util.Collections;
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
import static com.hazelcast.simulator.worker.performance.IntervalStats.INTERVAL_LATENCY_PERCENTILE;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Responsible for storing and formatting performance metrics from Simulator workers.
 */
public class PerformanceStatsCollector {

    public static final int OPERATION_COUNT_FORMAT_LENGTH = 14;
    public static final int THROUGHPUT_FORMAT_LENGTH = 12;
    public static final int LATENCY_FORMAT_LENGTH = 10;

    private static final long DISPLAY_LATENCY_AS_MICROS_MAX_VALUE = SECONDS.toMicros(1);

    // holds a map per Worker SimulatorAddress which contains the lastDelta IntervalStats per testCaseId
    private final ConcurrentMap<SimulatorAddress, WorkerPerformance> workerPerformanceInfoMap
            = new ConcurrentHashMap<SimulatorAddress, WorkerPerformance>();

    public void update(SimulatorAddress workerAddress, Map<String, IntervalStats> performanceStatsMap) {
        WorkerPerformance workerPerformance = workerPerformanceInfoMap.get(workerAddress);
        if (workerPerformance == null) {
            WorkerPerformance newInfo = new WorkerPerformance();
            WorkerPerformance foundInfo = workerPerformanceInfoMap.putIfAbsent(workerAddress, newInfo);
            workerPerformance = foundInfo == null ? newInfo : foundInfo;
        }

        workerPerformance.updateAll(performanceStatsMap);
    }

    public String formatIntervalPerformanceNumbers(String testId) {
        IntervalStats latest = get(testId, false);
        if (latest.isEmpty() || latest.getOperationCount() < 1) {
            return "";
        }

        String latencyUnit = "µs";
        long latencyAvg = NANOSECONDS.toMicros(round(latest.getLatencyAvg()));
        long latency999Percentile = NANOSECONDS.toMicros(latest.getLatency999Percentile());
        long latencyMax = NANOSECONDS.toMicros(latest.getLatencyMax());

        if (latencyAvg > DISPLAY_LATENCY_AS_MICROS_MAX_VALUE) {
            latencyUnit = "ms";
            latencyAvg = MICROSECONDS.toMillis(latencyAvg);
            latency999Percentile = MICROSECONDS.toMillis(latency999Percentile);
            latencyMax = MICROSECONDS.toMillis(latencyMax);
        }

        return format("%s ops %s ops/s %s %s (avg) %s %s (%sth) %s %s (max)",
                formatLong(latest.getOperationCount(), OPERATION_COUNT_FORMAT_LENGTH),
                formatDouble(latest.getIntervalThroughput(), THROUGHPUT_FORMAT_LENGTH),
                formatLong(latencyAvg, LATENCY_FORMAT_LENGTH),
                latencyUnit,
                formatLong(latency999Percentile, LATENCY_FORMAT_LENGTH),
                latencyUnit,
                INTERVAL_LATENCY_PERCENTILE,
                formatLong(latencyMax, LATENCY_FORMAT_LENGTH),
                latencyUnit);
    }

    IntervalStats get(String testCaseId, boolean aggregated) {
        // aggregate the IntervalStats instances from all Workers by adding values (since from different Workers)
        IntervalStats result = new IntervalStats();

        for (WorkerPerformance workerPerformance : workerPerformanceInfoMap.values()) {
            IntervalStats intervalStats = workerPerformance.get(testCaseId, aggregated);
            result.add(intervalStats);
        }

        return result;
    }

    public String detailedPerformanceInfo(String testId, long runningTimeMs) {
        IntervalStats totalIntervalStats = new IntervalStats();
        Map<SimulatorAddress, IntervalStats> agentPerformanceStatsMap = new HashMap<SimulatorAddress, IntervalStats>();
        calculatePerformanceStats(testId, totalIntervalStats, agentPerformanceStatsMap);

        long totalOperationCount = totalIntervalStats.getOperationCount();

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
            IntervalStats intervalStats = agentPerformanceStatsMap.get(address);

            long operationCount = intervalStats.getOperationCount();
            sb.append(format("  Agent %-15s %s%% %s ops %s ops/s\n",
                    address,
                    formatPercentage(operationCount, totalOperationCount),
                    formatLong(operationCount, OPERATION_COUNT_FORMAT_LENGTH),
                    formatDouble(operationCount / runningTimeSeconds, THROUGHPUT_FORMAT_LENGTH)));
        }
        return sb.toString();
    }

    void calculatePerformanceStats(String testId,
                                   IntervalStats totalIntervalStats,
                                   Map<SimulatorAddress, IntervalStats> agentPerformanceStatsMap) {

        for (Map.Entry<SimulatorAddress, WorkerPerformance> entry : workerPerformanceInfoMap.entrySet()) {
            SimulatorAddress workerAddress = entry.getKey();
            SimulatorAddress agentAddress = workerAddress.getParent();
            IntervalStats agentIntervalStats = agentPerformanceStatsMap.get(agentAddress);
            if (agentIntervalStats == null) {
                agentIntervalStats = new IntervalStats();
                agentPerformanceStatsMap.put(agentAddress, agentIntervalStats);
            }

            WorkerPerformance workerPerformance = entry.getValue();
            IntervalStats workerTestIntervalStats = workerPerformance.get(testId, true);
            if (workerTestIntervalStats != null) {
                agentIntervalStats.add(workerTestIntervalStats);
                totalIntervalStats.add(workerTestIntervalStats);
            }
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

    /**
     * Contains the performance info for a given worker.
     */
    private final class WorkerPerformance {
        // contains the performance per test. Key is test-id.
        private final ConcurrentMap<String, TestPerformance> testPerformanceMap
                = new ConcurrentHashMap<String, TestPerformance>();

        private void updateAll(Map<String, IntervalStats> deltas) {
            for (Map.Entry<String, IntervalStats> entry : deltas.entrySet()) {
                update(entry.getKey(), entry.getValue());
            }
        }

        private void update(String testId, IntervalStats delta) {
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

        private IntervalStats get(String testId, boolean aggregated) {
            TestPerformance testPerformance = testPerformanceMap.get(testId);
            if (testPerformance == null) {
                return new IntervalStats();
            }
            return aggregated ? testPerformance.aggregated : testPerformance.lastDelta;
        }
    }

    /**
     * Contains the latest and aggregated performance info.
     */
    private final class TestPerformance {
        private final IntervalStats aggregated;
        private final IntervalStats lastDelta;

        private TestPerformance(IntervalStats aggregated, IntervalStats lastDelta) {
            this.aggregated = aggregated;
            this.lastDelta = lastDelta;
        }

        private TestPerformance update(IntervalStats delta) {
            IntervalStats newAggregated = new IntervalStats(aggregated);
            newAggregated.add(delta, false);
            return new TestPerformance(newAggregated, delta);
        }
    }
}
