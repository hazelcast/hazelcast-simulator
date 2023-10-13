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
package com.hazelcast.simulator.worker.messages;

import com.google.gson.annotations.SerializedName;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.protocol.message.SimulatorMessage;

/**
 * Stops the {@link TestPhase#RUN} phase of a Simulator Test.
 * <p/>
 * The other phases of a test like setup, verify etc run as long as needed. In
 * most cases it will be short.
 *<p/>
 * But the run phase can take a very long time. Apart from the worker deciding it
 * has run long enough, e.g. because there was an exception, the coordinator
 * controls how long a test is going to run by sending the StopRunMessage.
 * If a test needs to run for 30m, then after 30m of the test being in the run
 * phase, the StopRunMessage is send.
 */
public class StopRunMessage implements SimulatorMessage {

    /**
     * The name of the test to stop.
     */
    @SerializedName("testId")
    private final String testId;

    public StopRunMessage(String testId) {
        this.testId = testId;
    }

    public String getTestId() {
        return testId;
    }

    @Override
    public String toString() {
        return "StopRunMessage{testId='" + testId + "'}";
    }
}
