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

import com.hazelcast.simulator.coordinator.Coordinator;
import com.hazelcast.simulator.coordinator.FailureCollector;
import com.hazelcast.simulator.coordinator.PerformanceStatsCollector;
import com.hazelcast.simulator.coordinator.TestPhaseListeners;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.protocol.operation.FailureOperation;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.PerformanceStatsOperation;
import com.hazelcast.simulator.protocol.operation.PhaseCompletedOperation;
import com.hazelcast.simulator.protocol.operation.RcDownloadOperation;
import com.hazelcast.simulator.protocol.operation.RcInstallOperation;
import com.hazelcast.simulator.protocol.operation.RcWorkerKillOperation;
import com.hazelcast.simulator.protocol.operation.RcTestRunOperation;
import com.hazelcast.simulator.protocol.operation.RcWorkerStartOperation;
import com.hazelcast.simulator.protocol.operation.RcTestStatusOperation;
import com.hazelcast.simulator.protocol.operation.RcTestStopOperation;
import com.hazelcast.simulator.protocol.operation.RcWorkerScriptOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.worker.Promise;
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

    private final FailureCollector failureCollector;
    private final TestPhaseListeners testPhaseListeners;
    private final PerformanceStatsCollector performanceStatsCollector;
    private final Coordinator coordinator;

    public CoordinatorOperationProcessor(Coordinator coordinator, FailureCollector failureCollector,
                                         TestPhaseListeners testPhaseListeners,
                                         PerformanceStatsCollector performanceStatsCollector) {
        this.coordinator = coordinator;
        this.failureCollector = failureCollector;
        this.testPhaseListeners = testPhaseListeners;
        this.performanceStatsCollector = performanceStatsCollector;
    }

    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    @Override
    protected void processOperation(OperationType operationType, SimulatorOperation operation,
                                    SimulatorAddress sourceAddress, Promise promise) throws Exception {
        switch (operationType) {
            case FAILURE:
                failureCollector.notify((FailureOperation) operation);
                break;
            case PHASE_COMPLETED:
                promise.answer(processPhaseCompletion((PhaseCompletedOperation) operation, sourceAddress));
                return;
            case PERFORMANCE_STATE:
                performanceStatsCollector.update(sourceAddress, ((PerformanceStatsOperation) operation).getPerformanceStats());
                break;
            case RC_INSTALL:
                coordinator.install(((RcInstallOperation) operation).getVersionSpec());
                break;
            case RC_WORKER_START:
                promise.answer(SUCCESS, coordinator.startWorkers((RcWorkerStartOperation) operation));
                return;
            case RC_TEST_RUN:
                coordinator.runSuite(((RcTestRunOperation) operation), promise);
                return;
            case RC_WORKER_KILL:
                promise.answer(SUCCESS, coordinator.killWorker((RcWorkerKillOperation) operation));
                return;
            case RC_TEST_STATUS:
                promise.answer(SUCCESS, coordinator.testStatus((RcTestStatusOperation) operation));
                return;
            case RC_TEST_STOP:
                coordinator.testStop((RcTestStopOperation) operation);
                break;
            case RC_EXIT:
                coordinator.exit();
                break;
            case RC_WORKER_SCRIPT:
                coordinator.workerScript((RcWorkerScriptOperation) operation);
                break;
            case RC_PRINT_LAYOUT:
                promise.answer(ResponseType.SUCCESS, coordinator.printLayout());
                return;
            case RC_DOWNLOAD:
                coordinator.download((RcDownloadOperation) operation);
                break;
            default:
                throw new ProcessException(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR);
        }
        promise.answer(SUCCESS);
    }

    private ResponseType processPhaseCompletion(PhaseCompletedOperation operation, SimulatorAddress sourceAddress) {
        if (!TEST.equals(sourceAddress.getAddressLevel())) {
            LOGGER.error(format("Retrieved PhaseCompletedOperation %s from %s", operation.getTestPhase(), sourceAddress));
            return EXCEPTION_DURING_OPERATION_EXECUTION;
        }
        int testIndex = sourceAddress.getTestIndex();
        SimulatorAddress workerAddress = sourceAddress.getParent();
        testPhaseListeners.onCompletion(testIndex, operation.getTestPhase(), workerAddress);
        return SUCCESS;
    }
}
