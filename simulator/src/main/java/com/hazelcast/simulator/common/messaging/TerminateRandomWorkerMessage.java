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
package com.hazelcast.simulator.common.messaging;

/**
 * This message terminates a random worker when sent to an agent.
 *
 * Termination is done by calling {@link Process#destroy()} from an agent, resulting in sending a SIGTERM signal when running on
 * Linux.
 */
@MessageSpec(value = "terminateWorker", description = "Indicates to an Agent to terminate a random Worker.")
public class TerminateRandomWorkerMessage extends Message {

    public TerminateRandomWorkerMessage(MessageAddress messageAddress) {
        super(messageAddress);
    }
}
