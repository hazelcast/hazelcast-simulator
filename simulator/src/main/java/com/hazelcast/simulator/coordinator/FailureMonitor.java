package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.test.Failure;
import com.hazelcast.util.EmptyStatement;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.appendText;

@SuppressWarnings("checkstyle:finalclass")
class FailureMonitor {

    private static final long THREAD_JOIN_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(5);

    private final FailureThread thread;
    private final AtomicBoolean started = new AtomicBoolean();

    FailureMonitor(AgentsClient agentsClient, String testSuiteId) {
        if (agentsClient == null) {
            throw new NullPointerException();
        }
        thread = new FailureThread(agentsClient, testSuiteId);
    }

    void start() {
        if (started.compareAndSet(false, true)) {
            thread.start();
        }
    }

    void shutdown() {
        if (started.compareAndSet(true, false)) {
            try {
                thread.isRunning = false;
                thread.interrupt();
                thread.join(THREAD_JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                EmptyStatement.ignore(e);
            }
        }
    }

    int getFailureCount() {
        if (!started.get()) {
            throw new IllegalStateException("FailureMonitor has not been started!");
        }
        return thread.getFailureCount();
    }

    boolean hasCriticalFailure(Set<Failure.Type> nonCriticalFailures) {
        if (!started.get()) {
            throw new IllegalStateException("FailureMonitor has not been started!");
        }
        return thread.hasCriticalFailure(nonCriticalFailures);
    }

    private static final class FailureThread extends Thread {

        private static final Logger LOGGER = Logger.getLogger(FailureMonitor.class);

        private final BlockingQueue<Failure> failureList = new LinkedBlockingQueue<Failure>();

        private final AgentsClient agentsClient;
        private final File file;

        private volatile boolean isRunning = true;

        private FailureThread(AgentsClient agentsClient, String testSuiteId) {
            super("FailureMonitorThread");
            setDaemon(true);

            this.agentsClient = agentsClient;
            this.file = new File("failures-" + testSuiteId + ".txt");
        }

        @Override
        public void run() {
            while (isRunning) {
                try {
                    sleepSeconds(1);
                    scan();
                } catch (Throwable e) {
                    LOGGER.fatal("Exception in FailureThread.run()", e);
                }
            }
        }

        private void scan() {
            for (Failure failure : agentsClient.getFailures()) {
                failureList.add(failure);
                LOGGER.warn(buildMessage(failure));
                appendText(failure.toString() + "\n", file);
            }
        }

        private String buildMessage(Failure failure) {
            StringBuilder sb = new StringBuilder();
            sb.append("Failure #").append(failureList.size()).append(" ");
            if (failure.hzAddress != null) {
                sb.append(' ');
                sb.append(failure.hzAddress);
                sb.append(' ');
            } else if (failure.agentAddress != null) {
                sb.append(' ');
                sb.append(failure.agentAddress);
                sb.append(' ');
            }
            sb.append(failure.testId);
            sb.append(' ');
            sb.append(failure.type);

            if (failure.cause != null) {
                String[] lines = failure.cause.split("\n");
                if (lines.length > 0) {
                    sb.append("[");
                    sb.append(lines[0]);
                    sb.append("]");
                }
            }

            return sb.toString();
        }

        private int getFailureCount() {
            return failureList.size();
        }

        private boolean hasCriticalFailure(Set<Failure.Type> nonCriticalFailures) {
            for (Failure failure : failureList) {
                if (!nonCriticalFailures.contains(failure.type)) {
                    return true;
                }
            }
            return false;
        }
    }
}
