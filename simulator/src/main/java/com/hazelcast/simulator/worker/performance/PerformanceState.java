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

import static java.lang.Math.max;

/**
 * Container to transfer performance states from a Simulator Worker to the Coordinator.
 *
 * Has methods to combine {@link PerformanceState} instances by adding or setting maximum values.
 */
public class PerformanceState {

    public static final double INTERVAL_LATENCY_PERCENTILE = 99.9;

    private static final long EMPTY_OPERATION_COUNT = -1;
    private static final double EMPTY_THROUGHPUT = -1;

    private long operationCount;
    private double intervalThroughput;
    private double totalThroughput;

    private double intervalAvgLatency;
    private long intervalMaxLatency;
    private long intervalPercentileLatency;

    /**
     * Creates an empty {@link PerformanceState} instance.
     */
    public PerformanceState() {
        this.operationCount = EMPTY_OPERATION_COUNT;
        this.intervalThroughput = EMPTY_THROUGHPUT;
    }

    /**
     * Creates a {@link PerformanceState} instance with values.
     *
     * @param operationCount            Operation count value.
     * @param intervalThroughput        Throughput value for an interval.
     * @param totalThroughput           Total throughput value.
     * @param intervalAvgLatency        Average latency for an interval.
     * @param intervalPercentileLatency Percentile latency for an interval ({@link PerformanceState#INTERVAL_LATENCY_PERCENTILE}).
     * @param intervalMaxLatency        Maximum latency for an interval.
     */
    public PerformanceState(long operationCount, double intervalThroughput, double totalThroughput,
                            double intervalAvgLatency, long intervalPercentileLatency, long intervalMaxLatency) {
        this.operationCount = operationCount;
        this.intervalThroughput = intervalThroughput;
        this.totalThroughput = totalThroughput;

        this.intervalAvgLatency = intervalAvgLatency;
        this.intervalPercentileLatency = intervalPercentileLatency;
        this.intervalMaxLatency = intervalMaxLatency;
    }

    /**
     * Combines two {@link PerformanceState} instances, e.g. from different Simulator Workers.
     *
     * @param other {@link PerformanceState} which should be added to this instance
     */
    public void add(PerformanceState other) {
        add(other, true);
    }

    /**
     * Combines {@link PerformanceState} instances, e.g. from different Simulator Workers.
     *
     * For the real-time performance monitor during the {@link com.hazelcast.simulator.test.TestPhase#RUN} the maximum values
     * should be set, so we get the maximum operation count and throughput values of all {@link PerformanceState} instances of
     * the last interval.
     *
     * For the total performance number and the performance per Simulator Agent, the added values should be set, so we get the
     * summed up operation count and throughput values.
     *
     * The method always sets the maximum values for latency.
     *
     * @param other                          {@link PerformanceState} which should be added to this instance
     * @param addOperationCountAndThroughput {@code true} if operation count and throughput should be added,
     *                                       {@code false} if the maximum value should be set
     */
    public void add(PerformanceState other, boolean addOperationCountAndThroughput) {
        if (other.isEmpty()) {
            return;
        }

        if (isEmpty()) {
            operationCount = other.operationCount;
            intervalThroughput = other.intervalThroughput;
            totalThroughput = other.totalThroughput;

            intervalAvgLatency = other.intervalAvgLatency;
            intervalPercentileLatency = other.intervalPercentileLatency;
            intervalMaxLatency = other.intervalMaxLatency;
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

            intervalAvgLatency = max(intervalAvgLatency, other.intervalAvgLatency);
            intervalPercentileLatency = max(intervalPercentileLatency, other.intervalPercentileLatency);
            intervalMaxLatency = max(intervalMaxLatency, other.intervalMaxLatency);
        }
    }

    /**
     * Returns if the {@link PerformanceState} instance is still empty.
     *
     * @return {@code true} if the {@link PerformanceState} instance is empty, {@code false} otherwise
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

    public double getIntervalAvgLatency() {
        return intervalAvgLatency;
    }

    public long getIntervalPercentileLatency() {
        return intervalPercentileLatency;
    }

    public long getIntervalMaxLatency() {
        return intervalMaxLatency;
    }

    @Override
    public String toString() {
        return "PerformanceState{"
                + "operationCount=" + operationCount
                + ", intervalThroughput=" + intervalThroughput
                + ", totalThroughput=" + totalThroughput
                + ", intervalAvgLatency=" + intervalAvgLatency
                + ", intervalPercentileLatency=" + intervalPercentileLatency
                + ", intervalMaxLatency=" + intervalMaxLatency
                + '}';
    }
}
