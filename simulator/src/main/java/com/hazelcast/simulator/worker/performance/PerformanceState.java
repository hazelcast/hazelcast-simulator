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

    private long intervalMaxLatency;
    private long intervalPercentileLatency;

    public PerformanceState() {
        this.operationCount = EMPTY_OPERATION_COUNT;
        this.intervalThroughput = EMPTY_THROUGHPUT;
    }

    public PerformanceState(long operationCount, double intervalThroughput, double totalThroughput,
                            long intervalPercentileLatency, long intervalMaxLatency) {
        this.operationCount = operationCount;
        this.intervalThroughput = intervalThroughput;
        this.totalThroughput = totalThroughput;

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

            intervalPercentileLatency = other.intervalPercentileLatency;
            intervalMaxLatency = other.intervalMaxLatency;
        } else {
            operationCount += other.operationCount;
            intervalThroughput += other.intervalThroughput;
            totalThroughput += other.totalThroughput;

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
                + ", intervalPercentileLatency=" + intervalPercentileLatency
                + ", intervalMaxLatency=" + intervalMaxLatency
                + '}';
    }
}
