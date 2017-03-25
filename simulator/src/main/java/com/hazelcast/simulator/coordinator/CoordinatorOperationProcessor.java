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
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.coordinator.operations.FailureOperation;
import com.hazelcast.simulator.protocol.OperationProcessor;
import com.hazelcast.simulator.protocol.Promise;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.worker.operations.IntervalStatsOperation;
import org.apache.log4j.Logger;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;

public class CoordinatorOperationProcessor implements OperationProcessor {
    private static final Logger LOGGER = Logger.getLogger(CoordinatorOperationProcessor.class);

    private final FailureCollector failureCollector;
    private final PerformanceStatsCollector performanceStatsCollector;

    public CoordinatorOperationProcessor(FailureCollector failureCollector,
                                         PerformanceStatsCollector performanceStatsCollector) {
        this.failureCollector = failureCollector;
        this.performanceStatsCollector = performanceStatsCollector;
    }

    @Override
    public void process(SimulatorOperation op, SimulatorAddress source, Promise promise) throws Exception {
        if (op instanceof FailureOperation) {
            failureCollector.notify((FailureOperation) op);
        } else if (op instanceof IntervalStatsOperation) {
            performanceStatsCollector.update(source, ((IntervalStatsOperation) op).getStatsMap());
        } else if (op instanceof LogOperation) {
            LogOperation logOperation = (LogOperation) op;
            LOGGER.log(logOperation.getLevel(), logOperation.getMessage());
        } else {
            throw new ProcessException("Unknown operation:" + op, UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR);
        }
        promise.answer(SUCCESS);
    }
}
