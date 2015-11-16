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
package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ExceptionLogger;
import com.hazelcast.simulator.protocol.operation.CreateTestOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.worker.TestContainer;
import com.hazelcast.simulator.worker.TestContextImpl;
import com.hazelcast.simulator.worker.Worker;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.utils.FileUtils.isValidFileName;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindProperties;
import static com.hazelcast.simulator.utils.TestUtils.getUserContextKeyFromTestId;
import static java.lang.String.format;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Worker.
 */
public class WorkerOperationProcessor extends OperationProcessor {

    private static final String DASHES = "---------------------------";
    private static final Logger LOGGER = Logger.getLogger(WorkerOperationProcessor.class);

    private ExceptionLogger exceptionLogger;

    private final ConcurrentMap<String, TestContainer> tests = new ConcurrentHashMap<String, TestContainer>();

    private final WorkerType type;
    private final HazelcastInstance hazelcastInstance;
    private final Worker worker;
    private final SimulatorAddress workerAddress;

    public WorkerOperationProcessor(ExceptionLogger exceptionLogger, WorkerType type, HazelcastInstance hazelcastInstance,
                                    Worker worker, SimulatorAddress workerAddress) {
        super(exceptionLogger);
        this.exceptionLogger = exceptionLogger;

        this.type = type;
        this.hazelcastInstance = hazelcastInstance;
        this.worker = worker;
        this.workerAddress = workerAddress;
    }

    public Collection<TestContainer> getTests() {
        return tests.values();
    }

    @Override
    protected ResponseType processOperation(OperationType operationType, SimulatorOperation operation,
                                            SimulatorAddress sourceAddress) throws Exception {
        switch (operationType) {
            case TERMINATE_WORKER:
                processTerminateWorker();
                break;
            case CREATE_TEST:
                processCreateTest((CreateTestOperation) operation);
                break;
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
        return SUCCESS;
    }

    private void processTerminateWorker() throws Exception {
        worker.shutdown();
    }

    private void processCreateTest(CreateTestOperation operation) throws Exception {
        TestCase testCase = operation.getTestCase();
        int testIndex = operation.getTestIndex();
        WorkerConnector workerConnector = worker.getWorkerConnector();
        if (workerConnector.getTest(testIndex) != null) {
            throw new IllegalStateException(format("Can't init TestCase: %s, another test with testIndex %d already exists",
                    operation, testIndex));
        }
        String testId = testCase.getId();
        if (tests.containsKey(testId)) {
            throw new IllegalStateException(format("Can't init TestCase: %s, another test with testId [%s] already exists",
                    operation, testId));
        }
        if (!testId.isEmpty() && !isValidFileName(testId)) {
            throw new IllegalArgumentException(format("Can't init TestCase: %s, testId [%s] is an invalid filename",
                    operation, testId));
        }

        LOGGER.info(format("%s Initializing test %s %s%n%s", DASHES, testId, DASHES, testCase));

        Object testInstance = CreateTestOperation.class.getClassLoader().loadClass(testCase.getClassname()).newInstance();
        bindProperties(testInstance, testCase, TestContainer.OPTIONAL_TEST_PROPERTIES);
        TestContextImpl testContext = new TestContextImpl(testId, hazelcastInstance);
        TestContainer testContainer = new TestContainer(testInstance, testContext, testCase);
        TestOperationProcessor processor = new TestOperationProcessor(exceptionLogger, worker, type, testIndex, testId,
                testContainer, workerAddress.getChild(testIndex));

        workerConnector.addTest(testIndex, processor);
        tests.put(testId, testContainer);

        if (type == WorkerType.MEMBER) {
            hazelcastInstance.getUserContext().put(getUserContextKeyFromTestId(testId), testInstance);
        }
    }
}
