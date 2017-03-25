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
 * Contains performance statistics for an interval.
 * <p>
 * Has methods to combine {@link IntervalStats} instances by adding or setting maximum values.
 *
 * There is a lot of stuff in there, but the thing most important is the operationCount (in a given time window).
 */
public class IntervalStats {

    public static final double INTERVAL_LATENCY_PERCENTILE = 99.9;

    private static final long EMPTY_OPERATION_COUNT = -1;
    private static final double EMPTY_THROUGHPUT = -1;

    private long operationCount;
    private double intervalThroughput;
    private double totalThroughput;
    // average latency in nanos
    private double latencyAvg;
    // max latency in nanos
    private long latencyMax;
    // 99.9 percentile in nanos
    private long latency999Percentile;

    /**
     * Creates an empty {@link IntervalStats} instance.
     */
    public IntervalStats() {
        this.operationCount = EMPTY_OPERATION_COUNT;
        this.intervalThroughput = EMPTY_THROUGHPUT;
    }


    /**
     * Creates a {@link IntervalStats} instance with values.
     *
     * @param operationCount                    Operation count value.
     * @param intervalThroughput                Throughput value for an interval.
     * @param totalThroughput                   Total throughput value.
     * @param latencyAvg           Average latency for an interval.
     * @param latency999Percentile 99.9 Percentile latency for an interval
     *                                          ({@link IntervalStats#INTERVAL_LATENCY_PERCENTILE}).
     * @param latencyMax           Maximum latency for an interval.
     */
    public IntervalStats(long operationCount,
                         double intervalThroughput,
                         double totalThroughput,
                         double latencyAvg,
                         long latency999Percentile,
                         long latencyMax) {
        this.operationCount = operationCount;
        this.intervalThroughput = intervalThroughput;
        this.totalThroughput = totalThroughput;
        this.latencyAvg = latencyAvg;
        this.latency999Percentile = latency999Percentile;
        this.latencyMax = latencyMax;
    }

    public IntervalStats(IntervalStats original) {
        this.operationCount = original.operationCount;
        this.intervalThroughput = original.intervalThroughput;
        this.totalThroughput = original.totalThroughput;
        this.latencyAvg = original.latencyAvg;
        this.latency999Percentile = original.latency999Percentile;
        this.latencyMax = original.latencyMax;
    }

    /**
     * Combines two {@link IntervalStats} instances, e.g. from different Simulator Workers.
     *
     * @param other {@link IntervalStats} which should be added to this instance
     */
    public void add(IntervalStats other) {
        add(other, true);
    }

    /**
     * Combines {@link IntervalStats} instances, e.g. from different Simulator Workers.
     * <p>
     * For the real-time performance monitor during the {@link TestPhase#RUN} the
     * maximum value should be set, so we get the maximum operation count and throughput values of all {@link IntervalStats}
     * instances of the last interval.
     * <p>
     * For the total performance number and the performance per Simulator Agent, the added values should be set, so we get the
     * summed up operation count and throughput values.
     * <p>
     * The method always sets the maximum values for latency.
     *
     * @param other                          {@link IntervalStats} which should be added to this instance
     * @param addOperationCountAndThroughput {@code true} if operation count and throughput should be added,
     *                                       {@code false} if the maximum value should be set
     */
    public void add(IntervalStats other, boolean addOperationCountAndThroughput) {
        if (other.isEmpty()) {
            return;
        }

        if (isEmpty()) {
            operationCount = other.operationCount;
            intervalThroughput = other.intervalThroughput;
            totalThroughput = other.totalThroughput;

            latencyAvg = other.latencyAvg;
            latency999Percentile = other.latency999Percentile;
            latencyMax = other.latencyMax;
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

            latencyAvg = max(latencyAvg, other.latencyAvg);
            latency999Percentile = max(latency999Percentile, other.latency999Percentile);
            latencyMax = max(latencyMax, other.latencyMax);
        }
    }

    /**
     * Returns if the {@link IntervalStats} instance is still empty.
     *
     * @return {@code true} if the {@link IntervalStats} instance is empty, {@code false} otherwise
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

    public double getLatencyAvg() {
        return latencyAvg;
    }

    public long getLatency999Percentile() {
        return latency999Percentile;
    }

    public long getLatencyMax() {
        return latencyMax;
    }

    @Override
    public String toString() {
        return "IntervalStats{"
                + "operationCount=" + operationCount
                + ", intervalThroughput=" + intervalThroughput
                + ", totalThroughput=" + totalThroughput
                + ", intervalAvgLatencyNanos=" + latencyAvg
                + ", latency999Percentile=" + latency999Percentile
                + ", intervalMaxLatencyNanos=" + latencyMax
                + '}';
    }


    public static IntervalStats aggregateAll(IntervalStats... stats) {
        IntervalStats result = new IntervalStats();
        for (IntervalStats s : stats) {
            result.add(s);
        }
        return result;
    }
}
