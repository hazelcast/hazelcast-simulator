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
    private long previousCount = 0;
    public long previousTime = System.currentTimeMillis();

    public PerformanceMonitor(AgentsClient client) {
        this.client = client;
    }

    @Override
    public void run() {
        for (; ; ) {
            Utils.sleepSeconds(30);

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
        log.info(format("Performance is %s operations/second", performance));
        previousTime = currentMs;
        previousCount = currentCount;
    }
}
