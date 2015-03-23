package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.test.Failure;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.FileUtils.appendText;

class FailureMonitorThread extends Thread {
    private static final Logger LOGGER = Logger.getLogger(FailureMonitorThread.class);
    private final File file;
    private final Coordinator coordinator;

    public FailureMonitorThread(Coordinator coordinator) {
        super("FailureMonitorThread");

        if (coordinator == null) {
            throw new NullPointerException();
        }

        file = new File("failures-" + coordinator.testSuite.id + ".txt");

        this.coordinator = coordinator;
        this.setDaemon(true);
    }

    public void run() {
        for (; ; ) {
            try {
                //todo: this delay should be configurable.
                sleepSeconds(1);
                scan();
            } catch (Throwable e) {
                LOGGER.fatal(e);
            }
        }
    }

    private void scan() {
        List<Failure> failures = coordinator.agentsClient.getFailures();
        for (Failure failure : failures) {
            coordinator.failureList.add(failure);
            LOGGER.warn(buildMessage(failure));
            appendText(failure.toString() + "\n", file);
        }
    }

    private String buildMessage(Failure failure) {
        StringBuilder sb = new StringBuilder();
        sb.append("Failure #").append(coordinator.failureList.size()).append(" ");
        if (failure.workerAddress != null) {
            sb.append(' ');
            sb.append(failure.workerAddress);
            sb.append(' ');
        } else if (failure.agentAddress != null) {
            sb.append(' ');
            sb.append(failure.agentAddress);
            sb.append(' ');
        }
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
}
