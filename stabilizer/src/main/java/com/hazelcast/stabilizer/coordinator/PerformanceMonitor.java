package com.hazelcast.stabilizer.coordinator;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.coordinator.remoting.AgentClient;
import com.hazelcast.stabilizer.coordinator.remoting.AgentsClient;
import com.hazelcast.stabilizer.worker.commands.GetOperationCountCommand;

import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Responsible for collecting performance metrics from the agents and logging/storing it.
 */
public class PerformanceMonitor extends Thread {
    private final ILogger log = Logger.getLogger(PerformanceMonitor.class);
    private final NumberFormat performanceFormat = NumberFormat.getInstance(Locale.US);

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
                    countPerAgent += value;
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
        long totalOperations = 0;
        for (Map.Entry<AgentClient, Long> entry : operationCountPerAgent.entrySet()) {
            totalOperations += entry.getValue();
        }

        log.info("Total operations executed: " + totalOperations);

        for (Map.Entry<AgentClient, Long> entry : operationCountPerAgent.entrySet()) {
            AgentClient client = entry.getKey();
            long operationCount = entry.getValue();
            double percentage = 100 * (operationCount * 1.0d) / totalOperations;
            double performance = (operationCount * 1.0d) / duration;
            log.info("    Agent: " + client.getPublicAddress() + " operations: " + performanceFormat.format(operationCount)
                    + " operations/second: " + performanceFormat.format(performance)
                    + " share: " + performanceFormat.format(percentage) + " %\n");
        }

    }
}
