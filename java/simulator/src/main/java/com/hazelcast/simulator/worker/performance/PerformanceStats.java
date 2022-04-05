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
package com.hazelcast.simulator.worker.performance;

import com.hazelcast.simulator.common.TestPhase;

import static java.lang.Math.max;

/**
 * Container to transfer performance statistics for some time window.
 * <p>
 * Has methods to combine {@link PerformanceStats} instances by adding or setting maximum values.
 *
 * There is a lot of stuff in there, but the thing most important is the operationCount (in a given time window).
 */
public class PerformanceStats {

    public static final double INTERVAL_LATENCY_PERCENTILE = 99.9;

    private static final long EMPTY_OPERATION_COUNT = -1;
    private static final double EMPTY_THROUGHPUT = -1;

    private long operationCount;
    private double intervalThroughput;
    private double totalThroughput;
    private double intervalLatencyAvgNanos;
    private long intervalLatencyMaxNanos;
    private long intervalLatency999PercentileNanos;

    /**
     * Creates an empty {@link PerformanceStats} instance.
     */
    public PerformanceStats() {
        this.operationCount = EMPTY_OPERATION_COUNT;
        this.intervalThroughput = EMPTY_THROUGHPUT;
    }


    /**
     * Creates a {@link PerformanceStats} instance with values.
     *
     * @param operationCount                    Operation count value.
     * @param intervalThroughput                Throughput value for an interval.
     * @param totalThroughput                   Total throughput value.
     * @param intervalLatencyAvgNanos           Average latency for an interval.
     * @param intervalLatency999PercentileNanos 99.9 Percentile latency for an interval
     *                                          ({@link PerformanceStats#INTERVAL_LATENCY_PERCENTILE}).
     * @param intervalLatencyMaxNanos           Maximum latency for an interval.
     */
    public PerformanceStats(long operationCount,
                            double intervalThroughput,
                            double totalThroughput,
                            double intervalLatencyAvgNanos,
                            long intervalLatency999PercentileNanos,
                            long intervalLatencyMaxNanos) {
        this.operationCount = operationCount;
        this.intervalThroughput = intervalThroughput;
        this.totalThroughput = totalThroughput;
        this.intervalLatencyAvgNanos = intervalLatencyAvgNanos;
        this.intervalLatency999PercentileNanos = intervalLatency999PercentileNanos;
        this.intervalLatencyMaxNanos = intervalLatencyMaxNanos;
    }

    public PerformanceStats(PerformanceStats original) {
        this.operationCount = original.operationCount;
        this.intervalThroughput = original.intervalThroughput;
        this.totalThroughput = original.totalThroughput;
        this.intervalLatencyAvgNanos = original.intervalLatencyAvgNanos;
        this.intervalLatency999PercentileNanos = original.intervalLatency999PercentileNanos;
        this.intervalLatencyMaxNanos = original.intervalLatencyMaxNanos;
    }

    /**
     * Combines two {@link PerformanceStats} instances, e.g. from different Simulator Workers.
     *
     * @param other {@link PerformanceStats} which should be added to this instance
     */
    public void add(PerformanceStats other) {
        add(other, true);
    }

    /**
     * Combines {@link PerformanceStats} instances, e.g. from different Simulator Workers.
     * <p>
     * For the real-time performance monitor during the {@link TestPhase#RUN} the
     * maximum value should be set, so we get the maximum operation count and throughput values of all {@link PerformanceStats}
     * instances of the last interval.
     * <p>
     * For the total performance number and the performance per Simulator Agent, the added values should be set, so we get the
     * summed up operation count and throughput values.
     * <p>
     * The method always sets the maximum values for latency.
     *
     * @param other                          {@link PerformanceStats} which should be added to this instance
     * @param addOperationCountAndThroughput {@code true} if operation count and throughput should be added,
     *                                       {@code false} if the maximum value should be set
     */
    public void add(PerformanceStats other, boolean addOperationCountAndThroughput) {
        if (other.isEmpty()) {
            return;
        }

        if (isEmpty()) {
            operationCount = other.operationCount;
            intervalThroughput = other.intervalThroughput;
            totalThroughput = other.totalThroughput;

            intervalLatencyAvgNanos = other.intervalLatencyAvgNanos;
            intervalLatency999PercentileNanos = other.intervalLatency999PercentileNanos;
            intervalLatencyMaxNanos = other.intervalLatencyMaxNanos;
        } else {
            if (addOperationCountAndThroughput) {
                operationCount += other.operationCount;
                intervalThroughput += other.intervalThroughput;
                totalThroughput += other.totalThroughput;
            } else {
                operationCount = max(operationCount, other.operationCount);
                intervalThroughput = max(intervalThroughput, other.intervalThroughput);
                totalThroughput = max(totalThroughput, other.totalThroughput);
            }

            intervalLatencyAvgNanos = max(intervalLatencyAvgNanos, other.intervalLatencyAvgNanos);
            intervalLatency999PercentileNanos = max(intervalLatency999PercentileNanos, other.intervalLatency999PercentileNanos);
            intervalLatencyMaxNanos = max(intervalLatencyMaxNanos, other.intervalLatencyMaxNanos);
        }
    }

    /**
     * Returns if the {@link PerformanceStats} instance is still empty.
     *
     * @return {@code true} if the {@link PerformanceStats} instance is empty, {@code false} otherwise
     */
    public boolean isEmpty() {
        return (operationCount == EMPTY_OPERATION_COUNT && intervalThroughput == EMPTY_THROUGHPUT);
    }

    public double getTotalThroughput() {
        return totalThroughput;
    }

    public long getOperationCount() {
        return operationCount;
    }

    public double getIntervalThroughput() {
        return intervalThroughput;
    }

    public double getIntervalLatencyAvgNanos() {
        return intervalLatencyAvgNanos;
    }

    public long getIntervalLatency999PercentileNanos() {
        return intervalLatency999PercentileNanos;
    }

    public long getIntervalLatencyMaxNanos() {
        return intervalLatencyMaxNanos;
    }

    @Override
    public String toString() {
        return "PerformanceStats{"
                + "operationCount=" + operationCount
                + ", intervalThroughput=" + intervalThroughput
                + ", totalThroughput=" + totalThroughput
                + ", intervalAvgLatencyNanos=" + intervalLatencyAvgNanos
                + ", intervalLatency999PercentileNanos=" + intervalLatency999PercentileNanos
                + ", intervalMaxLatencyNanos=" + intervalLatencyMaxNanos
                + '}';
    }


    public static PerformanceStats aggregateAll(PerformanceStats... stats) {
        PerformanceStats result = new PerformanceStats();
        for (PerformanceStats s : stats) {
            result.add(s);
        }
        return result;
    }
}
