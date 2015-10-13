package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.test.FailureType;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import static com.hazelcast.simulator.utils.FileUtils.appendText;

/**
 * Responsible for storing and formatting failures from Simulator workers.
 */
public class FailureContainer {

    private static final Logger LOGGER = Logger.getLogger(FailureContainer.class);

    private final BlockingQueue<FailureOperation> failureOperations = new LinkedBlockingQueue<FailureOperation>();
    private final ConcurrentMap<String, FailureType> finishedWorkersList = new ConcurrentHashMap<String, FailureType>();

    private final File file;

    public FailureContainer(String testSuiteId) {
        this.file = new File("failures-" + testSuiteId + ".txt");
    }

    public int getFailureCount() {
        return failureOperations.size();
    }

    public boolean hasCriticalFailure(Set<FailureType> nonCriticalFailures) {
        for (FailureOperation operation : failureOperations) {
            if (!nonCriticalFailures.contains(operation.getType())) {
                return true;
            }
        }
        return false;
    }

    public Queue<FailureOperation> getFailureOperations() {
        return failureOperations;
    }

    public Set<String> getFinishedWorkers() {
        return finishedWorkersList.keySet();
    }

    public void addFailureOperation(FailureOperation operation) {
        FailureType failureType = operation.getType();
        if (failureType.isWorkerFinishedFailure()) {
            finishedWorkersList.put(operation.getWorkerAddress(), failureType);
        }
        if (failureType.isPoisonPill()) {
            return;
        }

        failureOperations.add(operation);

        LOGGER.warn(operation.getLogMessage(failureOperations.size()));
        appendText(operation.getFileMessage(), file);
    }
}
