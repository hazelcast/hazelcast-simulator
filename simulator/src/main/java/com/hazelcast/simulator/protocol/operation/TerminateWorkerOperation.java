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

import com.google.gson.annotations.SerializedName;
import com.hazelcast.simulator.worker.Worker;

/**
 * Initiates the shutdown process of the Worker.
 */
public class TerminateWorkerOperation implements SimulatorOperation {

    /**
     * Defines a delay for {@link Worker} for the shutdown, to give Hazelcast clients enough
     * time to disconnect gracefully, before the Hazelcast members are gone.
     *
     * Client Workers can ignore this parameter.
     */
    @SerializedName("memberWorkerShutdownDelaySeconds")
    private final int memberWorkerShutdownDelaySeconds;

    /**
     * Defines if Java Workers should shutdown their process (e.g. via {@link System#exit(int)}).
     *
     * This is done to ensure that the Worker process is killed, even if some Threads are non-responsive.
     * In Java Workers this is also used to shutdown log4j, to ensure flushing of the logs.
     *
     * This switch should not be set in integration tests, otherwise the logging will be gone in
     * subsequent tests. This may also kill the JVM of the test itself.
     */
    @SerializedName("ensureProcessShutdown")
    private final boolean ensureProcessShutdown;

    public TerminateWorkerOperation(int memberWorkerShutdownDelaySeconds, boolean ensureProcessShutdown) {
        this.memberWorkerShutdownDelaySeconds = memberWorkerShutdownDelaySeconds;
        this.ensureProcessShutdown = ensureProcessShutdown;
    }

    public int getMemberWorkerShutdownDelaySeconds() {
        return memberWorkerShutdownDelaySeconds;
    }

    public boolean isEnsureProcessShutdown() {
        return ensureProcessShutdown;
    }
}
