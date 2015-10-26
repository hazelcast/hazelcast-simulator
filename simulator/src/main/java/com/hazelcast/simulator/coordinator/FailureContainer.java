/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.FailureType;
import com.hazelcast.simulator.test.TestSuite;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.FileUtils.appendText;

/**
 * Responsible for storing and formatting failures from Simulator workers.
 */
public class FailureContainer {

    private static final Logger LOGGER = Logger.getLogger(FailureContainer.class);

    private final BlockingQueue<FailureOperation> failureOperations = new LinkedBlockingQueue<FailureOperation>();
    private final ConcurrentMap<SimulatorAddress, FailureType> finishedWorkers
            = new ConcurrentHashMap<SimulatorAddress, FailureType>();

    private final AtomicBoolean hasCriticalFailure = new AtomicBoolean();
    private final ConcurrentMap<String, Boolean> hasCriticalFailuresMap = new ConcurrentHashMap<String, Boolean>();

    private final File file;
    private final ComponentRegistry componentRegistry;
    private final Set<FailureType> nonCriticalFailures;

    public FailureContainer(TestSuite testSuite, ComponentRegistry componentRegistry) {
        this(testSuite.getId(), componentRegistry, testSuite.getTolerableFailures());
    }

    public FailureContainer(String testSuiteId, ComponentRegistry componentRegistry) {
        this(testSuiteId, componentRegistry, Collections.<FailureType>emptySet());
    }

    public FailureContainer(String testSuiteId, ComponentRegistry componentRegistry, Set<FailureType> nonCriticalFailures) {
        this.file = new File("failures-" + testSuiteId + ".txt");
        this.componentRegistry = componentRegistry;
        this.nonCriticalFailures = nonCriticalFailures;
    }

    public int getFailureCount() {
        return failureOperations.size();
    }

    public boolean hasCriticalFailure() {
        return hasCriticalFailure.get();
    }

    public boolean hasCriticalFailure(String testId) {
        return hasCriticalFailuresMap.containsKey(testId);
    }

    public Queue<FailureOperation> getFailureOperations() {
        return failureOperations;
    }

    public Set<SimulatorAddress> getFinishedWorkers() {
        return finishedWorkers.keySet();
    }

    public void addFailureOperation(FailureOperation operation) {
        FailureType failureType = operation.getType();
        if (failureType.isWorkerFinishedFailure()) {
            SimulatorAddress workerAddress = operation.getWorkerAddress();
            finishedWorkers.put(workerAddress, failureType);
            componentRegistry.removeWorker(workerAddress);
        }
        if (failureType.isPoisonPill()) {
            return;
        }

        failureOperations.add(operation);

        if (!nonCriticalFailures.contains(failureType)) {
            hasCriticalFailure.set(true);
            String testId = operation.getTestId();
            if (testId != null) {
                hasCriticalFailuresMap.put(testId, true);
            }
        }

        LOGGER.error(operation.getLogMessage(failureOperations.size()));
        appendText(operation.getFileMessage(), file);
    }
}
