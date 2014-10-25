package com.hazelcast.stabilizer.coordinator;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.coordinator.remoting.AgentClient;
import com.hazelcast.stabilizer.coordinator.remoting.AgentsClient;
import com.hazelcast.stabilizer.worker.commands.GetOperationCountCommand;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Responsible for collecting performance metrics from the agents and logging/storing it.
 */
public class PerformanceMonitor extends Thread {
    private static final AtomicBoolean performanceWritten = new AtomicBoolean();
    private static final ILogger log = Logger.getLogger(PerformanceMonitor.class);

    private final AgentsClient client;
    private final Coordinator coordinator;
    private final ConcurrentMap<AgentClient, Long> operationCountPerAgent = new ConcurrentHashMap<AgentClient, Long>();
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
        GetOperationCountCommand command = new GetOperationCountCommand();
        Map<AgentClient, List<Long>> result = client.executeOnAllWorkersDetailed(command);
        long totalCount = 0;
        for (Map.Entry<AgentClient, List<Long>> entry : result.entrySet()) {
            AgentClient agentClient = entry.getKey();
            Long countPerAgent = operationCountPerAgent.get(agentClient);

            if (countPerAgent == null) {
                countPerAgent = 0l;
            }

            for (Long value : entry.getValue()) {
                if (value != null) {
                    totalCount += value;
                    countPerAgent = value;
                }
            }

            operationCountPerAgent.put(agentClient, countPerAgent);
        }

        long delta = totalCount - previousCount;
        long currentMs = System.currentTimeMillis();
        long durationMs = currentMs - previousTime;

        coordinator.performance = (delta * 1000d) / durationMs;
        coordinator.operationCount = totalCount;
        previousTime = currentMs;
        previousCount = totalCount;
    }

    public void logDetailedPerformanceInfo(int duration) {
        long operationCount = coordinator.operationCount;
        if (operationCount < 0) {
            log.info("Operation-count: not available");
            log.info("Performance: not available");
        } else {
            log.info("Operation-count: " + Utils.formatLong(operationCount, 0));
            double performance = (operationCount * 1.0d) / duration;
            log.info("Performance: " + Utils.formatDouble(performance, 0) + " ops/s");
        }

        if (performanceWritten.compareAndSet(false, true)) {
            double performance = (operationCount * 1.0d) / duration;
            Utils.appendText("" + performance + "\n", new File("performance.txt"));
        }

        for (Map.Entry<AgentClient, Long> entry : operationCountPerAgent.entrySet()) {
            AgentClient client = entry.getKey();
            long operationCountPerAgent = entry.getValue();
            double percentage = 100 * (operationCountPerAgent * 1.0d) / operationCount;
            double performance = (operationCountPerAgent * 1.0d) / duration;
            log.info("    Agent " + client.getPublicAddress() + " " + Utils.formatLong(operationCountPerAgent,15) + " ops "
                    + Utils.formatDouble(performance,15)
                    + " ops/s " + Utils.formatDouble(percentage,5) + "%");
        }
    }
}
