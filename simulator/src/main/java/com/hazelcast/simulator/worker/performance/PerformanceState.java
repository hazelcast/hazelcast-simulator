package com.hazelcast.simulator.worker.performance;

public class PerformanceState {

    public static final long EMPTY_OPERATION_COUNT = -1;
    public static final double EMPTY_THROUGHPUT = -1;

    private long operationCount;
    private double intervalThroughput;
    private double totalThroughput;

    public PerformanceState() {
        this.operationCount = EMPTY_OPERATION_COUNT;
        this.intervalThroughput = EMPTY_THROUGHPUT;
    }

    public PerformanceState(long operationCount, double intervalThroughput, double totalThroughput) {
        this.operationCount = operationCount;
        this.intervalThroughput = intervalThroughput;
        this.totalThroughput = totalThroughput;
    }

    public void add(PerformanceState other) {
        if (other.isEmpty()) {
            return;
        }

        if (isEmpty()) {
            operationCount = other.getOperationCount();
            intervalThroughput = other.getIntervalThroughput();
            totalThroughput = other.getTotalThroughput();
        } else {
            operationCount += other.getOperationCount();
            intervalThroughput += other.getIntervalThroughput();
            totalThroughput += other.getTotalThroughput();
        }
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

    public boolean isEmpty() {
        return operationCount == EMPTY_OPERATION_COUNT && intervalThroughput == EMPTY_THROUGHPUT;
    }

    @Override
    public String toString() {
        return "PerformanceState{"
                + "operationCount=" + operationCount
                + ", intervalThroughput=" + intervalThroughput
                + ", totalThroughput=" + totalThroughput
                + '}';
    }
}
