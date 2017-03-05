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
package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.protocol.OperationProcessor;
import com.hazelcast.simulator.protocol.Promise;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.ProcessException;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.operations.CreateTestOperation;
import com.hazelcast.simulator.worker.operations.ExecuteScriptOperation;
import com.hazelcast.simulator.worker.operations.StartPhaseOperation;
import com.hazelcast.simulator.worker.operations.StopRunOperation;
import com.hazelcast.simulator.worker.operations.TerminateWorkerOperation;
import com.hazelcast.simulator.worker.testcontainer.TestManager;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;

public class WorkerOperationProcessor implements OperationProcessor {

    private final TestManager testManager;
    private final Worker worker;
    private final ScriptExecutor scriptExecutor;

    public WorkerOperationProcessor(Worker worker,
                                    TestManager testManager,
                                    ScriptExecutor scriptExecutor) {
        this.worker = worker;
        this.testManager = testManager;
        this.scriptExecutor = scriptExecutor;
    }

    @Override
    public void process(SimulatorOperation op, SimulatorAddress source, Promise promise) throws Exception {
        try {
            if (op instanceof TerminateWorkerOperation) {
                worker.shutdown(((TerminateWorkerOperation) op));
                promise.answer(SUCCESS);
            } else if (op instanceof CreateTestOperation) {
                testManager.createTest((CreateTestOperation) op);
                promise.answer(SUCCESS);
            } else if (op instanceof ExecuteScriptOperation) {
                scriptExecutor.execute((ExecuteScriptOperation) op, promise);
            } else if (op instanceof StartPhaseOperation) {
                testManager.startTestPhase((StartPhaseOperation) op, promise);
            } else if (op instanceof StopRunOperation) {
                testManager.stopRun((StopRunOperation) op);
                promise.answer(SUCCESS);
            } else {
                throw new ProcessException("Unknown operation:" + op, UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR);
            }
        } catch (Exception e) {
            // any uncaught exception we'll feed back into the ExceptionReporter.
            ExceptionReporter.report(null, e);
            throw e;
        }
    }
}
