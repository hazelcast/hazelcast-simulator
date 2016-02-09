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
package com.hazelcast.simulator.protocol.operation;

public class TerminateWorkerOperation implements SimulatorOperation {

    private final int memberWorkerShutdownDelaySeconds;
    private final boolean shutdownLog4j;

    public TerminateWorkerOperation(int memberWorkerShutdownDelaySeconds, boolean shutdownLog4j) {
        this.memberWorkerShutdownDelaySeconds = memberWorkerShutdownDelaySeconds;
        this.shutdownLog4j = shutdownLog4j;
    }

    public int getMemberWorkerShutdownDelaySeconds() {
        return memberWorkerShutdownDelaySeconds;
    }

    public boolean isShutdownLog4j() {
        return shutdownLog4j;
    }
}
