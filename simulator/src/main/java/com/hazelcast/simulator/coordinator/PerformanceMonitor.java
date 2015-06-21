package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.coordinator.remoting.AgentClient;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.worker.commands.GetOperationCountCommand;
import com.hazelcast.util.EmptyStatement;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
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
class PerformanceMonitor {

    private final PerformanceThread thread;
    private final AtomicBoolean started = new AtomicBoolean();

    PerformanceMonitor(AgentsClient agentsClient) {
        if (agentsClient == null) {
            throw new NullPointerException();
        }
        thread = new PerformanceThread(agentsClient);
    }

    void start() {
        if (started.compareAndSet(false, true)) {
            thread.start();
        }
    }

    void stop() {
        if (started.compareAndSet(true, false)) {
            try {
                thread.isRunning = false;
                thread.interrupt();
                thread.join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                EmptyStatement.ignore(e);
            }
        }
    }

    void logDetailedPerformanceInfo(int duration) {
        if (started.get()) {
            thread.logDetailedPerformanceInfo(duration);
        }
    }

    String getPerformanceNumbers() {
        if (started.get()) {
            return thread.getPerformanceNumbers();
        }
        return "";
    }

    private static class PerformanceThread extends Thread {

        private static final AtomicBoolean PERFORMANCE_WRITTEN = new AtomicBoolean();
        private static final Logger LOGGER = Logger.getLogger(PerformanceMonitor.class);

        private final ConcurrentMap<AgentClient, Long> operationCountPerAgent = new ConcurrentHashMap<AgentClient, Long>();

        private final AgentsClient agentsClient;

        private long previousTime = System.currentTimeMillis();
        private long previousCount;

        private volatile double performance;
        private volatile long operationCount;
        private volatile boolean isRunning = true;

        public PerformanceThread(AgentsClient agentsClient) {
            super("PerformanceThread");
            setDaemon(true);

            this.agentsClient = agentsClient;
        }

        @Override
        public void run() {
            while (isRunning) {
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

        private void checkPerformance() throws TimeoutException {
            GetOperationCountCommand command = new GetOperationCountCommand();
            Map<AgentClient, List<Long>> result = agentsClient.executeOnAllWorkersDetailed(command);
            long totalCount = 0;
            for (Map.Entry<AgentClient, List<Long>> entry : result.entrySet()) {
                AgentClient agentClient = entry.getKey();

                long countPerAgent = 0;
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

            performance = (delta * 1000d) / durationMs;
            operationCount = totalCount;
            previousTime = currentMs;
            previousCount = totalCount;
        }

        private void logDetailedPerformanceInfo(int duration) {
            if (!PERFORMANCE_WRITTEN.compareAndSet(false, true)) {
                return;
            }

            long totalOperationCount = operationCount;
            if (totalOperationCount < 0) {
                LOGGER.info("Performance information is not available!");
                return;
            }

            appendText(performance + "\n", new File("performance.txt"));
            if (totalOperationCount > 0) {
                double performance = (totalOperationCount * 1.0d) / duration;
                LOGGER.info(format("Total performance       %s%% %s ops %s ops/s",
                        formatDouble(100, 7),
                        formatLong(totalOperationCount, 15),
                        formatDouble(performance, 15)));
            }

            for (Map.Entry<AgentClient, Long> entry : operationCountPerAgent.entrySet()) {
                AgentClient client = entry.getKey();
                long operationCountPerAgent = entry.getValue();
                double percentage = 100 * (operationCountPerAgent * 1.0d) / totalOperationCount;
                performance = (operationCountPerAgent * 1.0d) / duration;
                LOGGER.info(format("  Agent %-15s %s%% %s ops %s ops/s",
                        client.getPublicAddress(),
                        formatDouble(percentage, 7),
                        formatLong(operationCountPerAgent, 15),
                        formatDouble(performance, 15)));
            }
        }

        private String getPerformanceNumbers() {
            if (operationCount < 0) {
                return " (performance not available)";
            }
            return String.format("%s ops %s ops/s",
                    formatLong(operationCount, 15),
                    formatDouble(performance, 15)
            );
        }
    }
}
