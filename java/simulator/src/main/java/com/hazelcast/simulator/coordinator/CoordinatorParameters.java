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

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestPhase;

import java.io.File;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;

/**
 * Parameters for Simulator Coordinator.
 */
public class CoordinatorParameters {

    private File runPath;
    private TestPhase lastTestPhaseToSync = TestPhase.getLastTestPhase();

    private SimulatorProperties simulatorProperties;
    private boolean skipDownload;
    private boolean skipShutdownHook;
    private int workerVmStartupDelayMs;
    private String runId;

    public CoordinatorParameters() {
    }

    public File getRunPath() {
        return runPath;
    }

    public String getRunId() {
        return runId;
    }

    public static String toSHA1(String s) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        md.reset();
        md.update(s.getBytes());
        return String.format("%040x", new BigInteger(1, md.digest()));
    }

    public CoordinatorParameters setRunPath(String runPath) {
        checkNotNull(runPath, "runPath can't be null");

        this.runPath = new File(runPath);
        this.runPath.mkdirs();
        this.runId = toSHA1(runPath);
        if (simulatorProperties != null) {
            simulatorProperties.set("RUN_ID", runId);
        }
        return this;
    }

    public TestPhase getLastTestPhaseToSync() {
        return lastTestPhaseToSync;
    }

    public CoordinatorParameters setLastTestPhaseToSync(TestPhase lastTestPhaseToSync) {
        this.lastTestPhaseToSync = lastTestPhaseToSync;
        return this;
    }

    public SimulatorProperties getSimulatorProperties() {
        return simulatorProperties;
    }

    public CoordinatorParameters setSimulatorProperties(SimulatorProperties simulatorProperties) {
        this.simulatorProperties = simulatorProperties;
        if (runId != null) {
            simulatorProperties.set("RUN_ID", runId);
        }
        return this;
    }

    public boolean skipShutdownHook() {
        return skipShutdownHook;
    }

    public CoordinatorParameters setSkipShutdownHook(boolean skipShutdownHook) {
        this.skipShutdownHook = skipShutdownHook;
        return this;
    }

    public int getWorkerVmStartupDelayMs() {
        return workerVmStartupDelayMs;
    }

    public CoordinatorParameters setWorkerVmStartupDelayMs(int workerVmStartupDelayMs) {
        this.workerVmStartupDelayMs = workerVmStartupDelayMs;
        return this;
    }
}
