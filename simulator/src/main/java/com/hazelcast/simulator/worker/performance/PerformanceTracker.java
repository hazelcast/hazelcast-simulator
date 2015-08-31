package com.hazelcast.simulator.worker.performance;

import com.hazelcast.simulator.worker.commands.PerformanceState;

import static java.util.concurrent.TimeUnit.SECONDS;

public class PerformanceTracker {
    private long lastOperationCount;
    private long startedTimestamp;
    private long lastTimestamp;

    public void start() {
        long now = System.currentTimeMillis();
        startedTimestamp = now;
        lastTimestamp = now;
    }

    public PerformanceState update(long currentOperationalCount) {
        if (currentOperationalCount == -1) {
            return new PerformanceState();
        }
        long now = System.currentTimeMillis();

        long opDelta = currentOperationalCount - lastOperationCount;
        long intervalTimeDelta = now - lastTimestamp;
        long totalTimeDelta = now - startedTimestamp;
        double intervalThroughput = (opDelta * SECONDS.toMillis(1)) / intervalTimeDelta;
        double totalThroughput = (currentOperationalCount * SECONDS.toMillis(1) / totalTimeDelta);
        lastOperationCount = currentOperationalCount;
        lastTimestamp = now;
        return new PerformanceState(currentOperationalCount, intervalThroughput, totalThroughput);
    }
}
