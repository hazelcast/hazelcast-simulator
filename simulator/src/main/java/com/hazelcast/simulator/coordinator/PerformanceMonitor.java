package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.coordinator.remoting.AgentClient;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.worker.commands.GetOperationCountCommand;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.formatDouble;
import static com.hazelcast.simulator.utils.CommonUtils.formatLong;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static java.lang.String.format;

/**
 * Responsible for collecting performance metrics from the agents and logging/storing it.
 */
public class PerformanceMonitor {
    private static final AtomicBoolean PERFORMANCE_WRITTEN = new AtomicBoolean();
    private static final Logger LOGGER = Logger.getLogger(PerformanceMonitor.class);

    public long previousTime = System.currentTimeMillis();
    private final AgentsClient client;
    private final Coordinator coordinator;
    private final ConcurrentMap<AgentClient, Long> operationCountPerAgent = new ConcurrentHashMap<AgentClient, Long>();
    private long previousCount;
    private final AtomicBoolean started = new AtomicBoolean();

    public PerformanceMonitor(Coordinator coordinator) {
        this.client = coordinator.agentsClient;
        this.coordinator = coordinator;
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            new PerformanceThread().start();
        }
    }

    class PerformanceThread extends Thread {

        public PerformanceThread() {
            super("PerformanceThread");
            setDaemon(true);
        }

        @Override
        public void run() {
            for (; ; ) {
                sleepSeconds(10);

                try {
                    checkPerformance();
                } catch (TimeoutException e) {
                    LOGGER.warn("There was a timeout retrieving performance information from the members.");
                } catch (Throwable cause) {
                    LOGGER.fatal(cause);
                }
            }
        }
    }

    private void checkPerformance() throws TimeoutException {
        GetOperationCountCommand command = new GetOperationCountCommand();
        Map<AgentClient, List<Long>> result = client.executeOnAllWorkersDetailed(command);
        long totalCount = 0;
        for (Map.Entry<AgentClient, List<Long>> entry : result.entrySet()) {
            AgentClient agentClient = entry.getKey();
            Long countPerAgent = operationCountPerAgent.get(agentClient);

            if (countPerAgent == null) {
                countPerAgent = 0L;
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
        if (!PERFORMANCE_WRITTEN.compareAndSet(false, true)) {
            return;
        }

        StringBuilder sb = new StringBuilder();

        long operationCount = coordinator.operationCount;
        if (operationCount < 0) {
            sb.append("Operations: not available");
        } else {
            double performance = (operationCount * 1.0d) / duration;
            sb.append(format("Total performance   %s%% %s ops %s ops/s%n",
                    formatDouble(100, 7),
                    formatLong(operationCount, 15),
                    formatDouble(performance, 15)));

            appendText(performance + "\n", new File("performance.txt"));
        }

        for (Map.Entry<AgentClient, Long> entry : operationCountPerAgent.entrySet()) {
            AgentClient client = entry.getKey();
            long operationCountPerAgent = entry.getValue();
            double performance = (operationCountPerAgent * 1.0d) / duration;
            double percentage = 100 * (operationCountPerAgent * 1.0d) / operationCount;
            sb.append(format("               Agent %s %s%% %s ops %s ops/s%n",
                    client.getPublicAddress(),
                    formatDouble(percentage, 7),
                    formatLong(operationCountPerAgent, 15),
                    formatDouble(performance, 15)));
        }

        LOGGER.info(sb.toString());
    }
}
