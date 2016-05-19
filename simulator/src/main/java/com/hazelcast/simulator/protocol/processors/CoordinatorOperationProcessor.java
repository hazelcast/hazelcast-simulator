/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.simulator.coordinator.FailureContainer;
import com.hazelcast.simulator.coordinator.PerformanceStatsContainer;
import com.hazelcast.simulator.coordinator.TestPhaseListeners;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.LocalExceptionLogger;
import com.hazelcast.simulator.protocol.operation.ExceptionOperation;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.PerformanceStatsOperation;
import com.hazelcast.simulator.protocol.operation.PhaseCompletedOperation;
import com.hazelcast.simulator.protocol.operation.RemoteControllerOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.core.AddressLevel.TEST;
import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static java.lang.String.format;

/**
 * An {@link OperationProcessor} implementation to process {@link SimulatorOperation} instances on a Simulator Coordinator.
 */
public class CoordinatorOperationProcessor extends AbstractOperationProcessor {

    private static final Logger LOGGER = Logger.getLogger(CoordinatorOperationProcessor.class);

    private final LocalExceptionLogger exceptionLogger;
    private final FailureContainer failureContainer;
    private final TestPhaseListeners testPhaseListeners;
    private final PerformanceStatsContainer performanceStatsContainer;
    private final CoordinatorRemoteControllerProcessor remoteControllerProcessor;

    public CoordinatorOperationProcessor(LocalExceptionLogger exceptionLogger, FailureContainer failureContainer,
                                         TestPhaseListeners testPhaseListeners,
                                         PerformanceStatsContainer performanceStatsContainer,
                                         CoordinatorRemoteControllerProcessor remoteControllerProcessor) {
        super(exceptionLogger);
        this.exceptionLogger = exceptionLogger;
        this.failureContainer = failureContainer;
        this.testPhaseListeners = testPhaseListeners;
        this.performanceStatsContainer = performanceStatsContainer;
        this.remoteControllerProcessor = remoteControllerProcessor;
    }

    @Override
    protected ResponseType processOperation(OperationType operationType, SimulatorOperation operation,
                                            SimulatorAddress sourceAddress) throws Exception {
        switch (operationType) {
            case EXCEPTION:
                processException((ExceptionOperation) operation);
                break;
            case FAILURE:
                processFailure((FailureOperation) operation);
                break;
            case PHASE_COMPLETED:
                return processPhaseCompletion((PhaseCompletedOperation) operation, sourceAddress);
            case PERFORMANCE_STATE:
                processPerformanceStats((PerformanceStatsOperation) operation, sourceAddress);
                break;
            case REMOTE_CONTROLLER:
                processRemoteController((RemoteControllerOperation) operation);
                break;
            default:
                return UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
        }
        return SUCCESS;
    }

    private void processException(ExceptionOperation operation) {
        exceptionLogger.logOperation(operation);
    }

    private void processFailure(FailureOperation operation) {
        failureContainer.addFailureOperation(operation);
    }

    private ResponseType processPhaseCompletion(PhaseCompletedOperation operation, SimulatorAddress sourceAddress) {
        if (!TEST.equals(sourceAddress.getAddressLevel())) {
            LOGGER.error(format("Retrieved PhaseCompletedOperation %s from %s", operation.getTestPhase(), sourceAddress));
            return EXCEPTION_DURING_OPERATION_EXECUTION;
        }
        int testIndex = sourceAddress.getTestIndex();
        SimulatorAddress workerAddress = sourceAddress.getParent();
        testPhaseListeners.updatePhaseCompletion(testIndex, operation.getTestPhase(), workerAddress);
        return SUCCESS;
    }

    private void processPerformanceStats(PerformanceStatsOperation operation, SimulatorAddress sourceAddress) {
        performanceStatsContainer.update(sourceAddress, operation.getPerformanceStats());
    }

    private void processRemoteController(RemoteControllerOperation operation) {
        remoteControllerProcessor.process(operation.getType());
    }
}
