package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.coordinator.remoting.AgentClient;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.worker.commands.GetPerformanceStateCommand;
import com.hazelcast.simulator.worker.commands.PerformanceState;
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
@SuppressWarnings("checkstyle:magicnumber")
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

    void logDetailedPerformanceInfo() {
        if (started.get()) {
            thread.logDetailedPerformanceInfo();
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

        private final ConcurrentMap<AgentClient, PerformanceState> performancePerAgent =
                new ConcurrentHashMap<AgentClient, PerformanceState>();

        private final AgentsClient agentsClient;

        //throughput in last measurement interval
        private double intervalThroughput;
        //overall throughput since test started
        private double totalThroughput;
        //total operation count since test started
        private long totalOperationCount;

        private volatile boolean isRunning = true;

        public PerformanceThread(AgentsClient agentsClient) {
            super("PerformanceThread");
            setDaemon(true);

            this.agentsClient = agentsClient;
        }

        @Override
        public void run() {
            while (isRunning) {
                sleepSeconds(1);

                try {
                    checkPerformance();
                } catch (TimeoutException e) {
                    LOGGER.warn("There was a timeout retrieving performance information from the members.");
                } catch (Throwable e) {
                    LOGGER.fatal("Exception in PerformanceThread.run()", e);
                }
            }
        }

        private synchronized void checkPerformance() throws TimeoutException {
            GetPerformanceStateCommand command = new GetPerformanceStateCommand();
            Map<AgentClient, List<PerformanceState>> result = agentsClient.executeOnAllWorkersDetailed(command);
            double intervalThroughput = 0;
            double totalThroughput = 0;
            long totalOperationCount = 0;
            for (Map.Entry<AgentClient, List<PerformanceState>> entry : result.entrySet()) {
                AgentClient agentClient = entry.getKey();
                long operationCountPerAgent = 0;
                PerformanceState agentPerformance = new PerformanceState();
                for (PerformanceState value : entry.getValue()) {
                    if (value != null && !value.isEmpty()) {
                        intervalThroughput += value.getIntervalThroughput();
                        totalThroughput += value.getTotalThroughput();
                        operationCountPerAgent += value.getOperationCount();
                        agentPerformance.add(value);
                    }
                }
                totalOperationCount += operationCountPerAgent;
                this.performancePerAgent.put(agentClient, agentPerformance);
            }

            this.totalOperationCount = totalOperationCount;
            this.intervalThroughput = intervalThroughput;
            this.totalThroughput = totalThroughput;
        }

        private synchronized void logDetailedPerformanceInfo() {
            if (!PERFORMANCE_WRITTEN.compareAndSet(false, true)) {
                return;
            }

            if (totalOperationCount < 0) {
                LOGGER.info("Performance information is not available!");
                return;
            }

            appendText(totalThroughput + "\n", new File("performance.txt"));
            if (totalOperationCount > 0) {
                LOGGER.info(format("Total performance       %s%% %s ops %s ops/s",
                        formatDouble(100, 7),
                        formatLong(totalOperationCount, 15),
                        formatDouble(totalThroughput, 15)));
            }

            for (Map.Entry<AgentClient, PerformanceState> entry : performancePerAgent.entrySet()) {
                AgentClient client = entry.getKey();
                PerformanceState performancePerAgent = entry.getValue();
                double percentage = 0;
                if (totalOperationCount > 0) {
                    percentage = 100 * (performancePerAgent.getOperationCount() * 1.0d) / totalOperationCount;
                }
                LOGGER.info(format("  Agent %-15s %s%% %s ops %s ops/s",
                        client.getPublicAddress(),
                        formatDouble(percentage, 7),
                        formatLong(performancePerAgent.getOperationCount(), 15),
                        formatDouble(performancePerAgent.getTotalThroughput(), 15)));
            }
        }

        private synchronized String getPerformanceNumbers() {
            if (intervalThroughput < 0) {
                return " (performance not available)";
            }
            return String.format("%s ops %s ops/s",
                    formatLong(totalOperationCount, 15),
                    formatDouble(intervalThroughput, 15)
            );
        }
    }
}
