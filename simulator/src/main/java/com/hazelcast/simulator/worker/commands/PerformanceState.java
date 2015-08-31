package com.hazelcast.simulator.worker.commands;

import java.io.Serializable;

public class PerformanceState implements Serializable {
    private static final long EMPTY_OPERATION_COUNT = -1;
    private static final double EMPTY_THROUGHPUT = -1;

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
        } else if (isEmpty()) {
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
        return "PerformanceResponse{"
                + "operationCount=" + operationCount
                + ", intervalThroughput=" + intervalThroughput
                + '}';
    }
}
