package com.hazelcast.stabilizer.coordinator;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.worker.testcommands.GetOperationCountTestCommand;

import java.util.List;

import static java.lang.String.format;

public class PerformanceMonitor extends Thread {
    private final ILogger log = Logger.getLogger(PerformanceMonitor.class);

    private final AgentsClient client;
    private final Coordinator coordinator;
    private long previousCount = 0;
    public long previousTime = System.currentTimeMillis();

    public PerformanceMonitor(Coordinator coordinator) {
        this.client = coordinator.agentsClient;
        this.coordinator = coordinator;
    }

    @Override
    public void run() {
        for (; ; ) {
            Utils.sleepSeconds(10);

            try {
                checkPerformance();
            } catch (Throwable t) {
                log.severe(t);
            }
        }
    }

    private void checkPerformance() {
        GetOperationCountTestCommand getOperationCountTestCommand = new GetOperationCountTestCommand();
        List<Long> result = (List<Long>) client.executeOnAllWorkers(getOperationCountTestCommand);
        long currentCount = 0;
        for (Long item : result) {
            currentCount += item;
        }

        long delta = currentCount - previousCount;
        long currentMs = System.currentTimeMillis();
        long durationMs = currentMs - previousTime;

        double performance = (delta * 1000d) / durationMs;
        coordinator.performance = performance;
        previousTime = currentMs;
        previousCount = currentCount;
    }
}
