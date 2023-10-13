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

import com.hazelcast.simulator.coordinator.messages.FailureMessage;
import com.hazelcast.simulator.protocol.MessageHandler;
import com.hazelcast.simulator.protocol.Promise;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.exception.HandleException;
import com.hazelcast.simulator.protocol.message.LogMessage;
import com.hazelcast.simulator.protocol.message.SimulatorMessage;
import com.hazelcast.simulator.worker.messages.PerformanceStatsMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CoordinatorMessageHandler implements MessageHandler {
    private static final Logger LOGGER = LogManager.getLogger(CoordinatorMessageHandler.class);

    private final FailureCollector failureCollector;
    private final PerformanceStatsCollector performanceStatsCollector;

    public CoordinatorMessageHandler(FailureCollector failureCollector,
                                     PerformanceStatsCollector performanceStatsCollector) {
        this.failureCollector = failureCollector;
        this.performanceStatsCollector = performanceStatsCollector;
    }

    @Override
    public void process(SimulatorMessage msg, SimulatorAddress source, Promise promise) throws Exception {
        if (msg instanceof FailureMessage) {
            failureCollector.notify((FailureMessage) msg);
        } else if (msg instanceof PerformanceStatsMessage) {
            performanceStatsCollector.update(source, ((PerformanceStatsMessage) msg).getPerformanceStats());
        } else if (msg instanceof LogMessage) {
            LogMessage logMsg = (LogMessage) msg;
            LOGGER.log(logMsg.getLevel(), logMsg.getMessage());
        } else {
            throw new HandleException("Unknown message:" + msg);
        }
        promise.answer("ok");
    }
}
