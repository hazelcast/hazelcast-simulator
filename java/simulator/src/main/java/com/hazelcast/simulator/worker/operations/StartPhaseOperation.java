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
package com.hazelcast.simulator.worker.operations;

import com.google.gson.annotations.SerializedName;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

/**
 * Starts a {@link TestPhase} of a Simulator test. Each test has phases to go through like setup, run, verify, teardown etc.
 * This operation triggers those phases.
 *
 * This Operation should only complete, when the phase has completed. If it takes e.g. 30minutes for the run phase to complete,
 * then only after 30 minutes a response is send.
 */
public class StartPhaseOperation implements SimulatorOperation {

    /**
     * The {@link TestPhase} which should be started.
     */
    @SerializedName("testPhase")
    private final String testPhase;

    /**
     * The name of the test to start.
     */
    @SerializedName("testId")
    private final String testId;

    public StartPhaseOperation(TestPhase testPhase, String testId) {
        this.testPhase = testPhase.name();
        this.testId = testId;
    }

    public TestPhase getTestPhase() {
        return TestPhase.valueOf(testPhase);
    }

    public String getTestId() {
        return testId;
    }

    @Override
    public String toString() {
        return "StartPhaseOperation{testPhase='" + testPhase + "', testId='" + testId + "'}";
    }
}
