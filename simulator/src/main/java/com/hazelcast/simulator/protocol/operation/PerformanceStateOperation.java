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

import com.hazelcast.simulator.worker.performance.PerformanceState;

import java.util.HashMap;
import java.util.Map;

/**
 * Sends a {@link PerformanceState} per running Simulator Test to the Coordinator, which contains the last snapshot of performance
 * numbers from that test.
 */
public class PerformanceStateOperation implements SimulatorOperation {

    /**
     * Map of {@link PerformanceState} per Simulator Test.
     */
    private final Map<String, PerformanceState> performanceStates = new HashMap<String, PerformanceState>();

    public void addPerformanceState(String testId, PerformanceState performanceState) {
        performanceStates.put(testId, performanceState);
    }

    public Map<String, PerformanceState> getPerformanceStates() {
        return performanceStates;
    }
}
