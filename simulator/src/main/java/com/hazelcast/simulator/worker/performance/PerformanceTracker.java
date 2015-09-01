package com.hazelcast.simulator.worker.performance;

import com.hazelcast.simulator.worker.commands.PerformanceState;

import static java.util.concurrent.TimeUnit.SECONDS;

public class PerformanceTracker {

    private static final long ONE_SECOND_IN_MILLIS = SECONDS.toMillis(1);

    private long startedTimestamp;
    private long lastTimestamp;
    private long lastOperationCount;

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
        double intervalThroughput = (opDelta * ONE_SECOND_IN_MILLIS) / (double) intervalTimeDelta;
        double totalThroughput = (currentOperationalCount * ONE_SECOND_IN_MILLIS / (double) totalTimeDelta);

        lastTimestamp = now;
        lastOperationCount = currentOperationalCount;

        return new PerformanceState(currentOperationalCount, intervalThroughput, totalThroughput);
    }
}
