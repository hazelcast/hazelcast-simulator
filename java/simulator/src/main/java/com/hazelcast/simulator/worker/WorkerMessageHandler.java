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

import com.hazelcast.simulator.protocol.MessageHandler;
import com.hazelcast.simulator.protocol.Promise;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.HandleException;
import com.hazelcast.simulator.protocol.message.SimulatorMessage;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.messages.CreateTestMessage;
import com.hazelcast.simulator.worker.messages.ExecuteScriptMessage;
import com.hazelcast.simulator.worker.messages.StartPhaseMessage;
import com.hazelcast.simulator.worker.messages.StopRunMessage;
import com.hazelcast.simulator.worker.messages.TerminateWorkerMessage;
import com.hazelcast.simulator.worker.testcontainer.TestManager;

public class WorkerMessageHandler implements MessageHandler {

    private final TestManager testManager;
    private final Worker worker;
    private final ScriptExecutor scriptExecutor;

    public WorkerMessageHandler(Worker worker, TestManager testManager,
                                ScriptExecutor scriptExecutor) {
        this.worker = worker;
        this.testManager = testManager;
        this.scriptExecutor = scriptExecutor;
    }

    @Override
    public void process(SimulatorMessage msg, SimulatorAddress source,
                        Promise promise) throws Exception {
        try {
            if (msg instanceof TerminateWorkerMessage) {
                worker.shutdown(((TerminateWorkerMessage) msg));
                promise.answer("ok");
            } else if (msg instanceof CreateTestMessage) {
                testManager.createTest((CreateTestMessage) msg);
                promise.answer("ok");
            } else if (msg instanceof ExecuteScriptMessage) {
                scriptExecutor.execute((ExecuteScriptMessage) msg, promise);
            } else if (msg instanceof StartPhaseMessage) {
                testManager.startTestPhase((StartPhaseMessage) msg, promise);
            } else if (msg instanceof StopRunMessage) {
                testManager.stopRun((StopRunMessage) msg);
                promise.answer("ok");
            } else {
                throw new HandleException("Unknown message:" + msg);
            }
        } catch (Throwable e) {
            // any uncaught exception we'll feed into the ExceptionReporter.
            ExceptionReporter.report(null, e);
            promise.answer(e);
        }
    }
}
