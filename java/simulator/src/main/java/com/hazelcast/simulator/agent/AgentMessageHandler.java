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
package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.agent.messages.CreateWorkerMessage;
import com.hazelcast.simulator.agent.messages.StartTimeoutDetectionMessage;
import com.hazelcast.simulator.agent.messages.StopTimeoutDetectionMessage;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessFailureMonitor;
import com.hazelcast.simulator.agent.workerprocess.WorkerProcessManager;
import com.hazelcast.simulator.protocol.MessageHandler;
import com.hazelcast.simulator.protocol.Promise;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.HandleException;
import com.hazelcast.simulator.protocol.message.SimulatorMessage;

class AgentMessageHandler implements MessageHandler {

    private final WorkerProcessManager processManager;
    private final WorkerProcessFailureMonitor failureMonitor;

    AgentMessageHandler(WorkerProcessManager processManager,
                        WorkerProcessFailureMonitor failureMonitor) {
        this.processManager = processManager;
        this.failureMonitor = failureMonitor;
    }

    @Override
    public void process(SimulatorMessage msg, SimulatorAddress source, Promise promise) throws Exception {
        if (msg instanceof CreateWorkerMessage) {
            processManager.launch((CreateWorkerMessage) msg, promise);
        } else if (msg instanceof StartTimeoutDetectionMessage) {
            failureMonitor.startTimeoutDetection();
            promise.answer("ok");
        } else if (msg instanceof StopTimeoutDetectionMessage) {
            failureMonitor.stopTimeoutDetection();
            promise.answer("ok");
        } else {
            throw new HandleException("Unknown message:" + msg);
        }
    }
}
