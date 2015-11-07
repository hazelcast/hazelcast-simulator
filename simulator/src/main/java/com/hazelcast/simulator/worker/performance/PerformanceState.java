/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

    public PerformanceState() {
        this.operationCount = EMPTY_OPERATION_COUNT;
        this.intervalThroughput = EMPTY_THROUGHPUT;
    }

    public PerformanceState(long operationCount, double intervalThroughput, double totalThroughput,
                            double intervalAvgLatency, long intervalPercentileLatency, long intervalMaxLatency) {
        this.operationCount = operationCount;
        this.intervalThroughput = intervalThroughput;
        this.totalThroughput = totalThroughput;

        this.intervalAvgLatency = intervalAvgLatency;
        this.intervalPercentileLatency = intervalPercentileLatency;
        this.intervalMaxLatency = intervalMaxLatency;
    }

    public void add(PerformanceState other) {
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
            operationCount += other.operationCount;
            intervalThroughput += other.intervalThroughput;
            totalThroughput += other.totalThroughput;

            intervalAvgLatency = max(intervalAvgLatency, other.intervalAvgLatency);
            intervalPercentileLatency = max(intervalPercentileLatency, other.intervalPercentileLatency);
            intervalMaxLatency = max(intervalMaxLatency, other.intervalMaxLatency);
        }
    }

    public boolean isEmpty() {
        return operationCount == EMPTY_OPERATION_COUNT && intervalThroughput == EMPTY_THROUGHPUT;
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
