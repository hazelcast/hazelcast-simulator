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
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.worker.performance.PerformanceStats;

import java.util.HashMap;
import java.util.Map;

/**
 * Sends a {@link PerformanceStats} per running Simulator Test to the Coordinator, which contains the last snapshot of performance
 * numbers from that test.
 *
 * This Operation is 'mandatory' to implement. The consequence of not implementing it is no performance information is
 * available on the coordinator for logging purposes.
 */
public class PerformanceStatsOperation implements SimulatorOperation {

    /**
     * Map of {@link PerformanceStats} per Simulator Test.
     *
     * The key is the id of the test.
     * The value is the PerformanceStats for that test.
     */
    @SerializedName("performanceStatsMap")
    private final Map<String, PerformanceStats> performanceStatsMap = new HashMap<>();

    public void addPerformanceStats(String testId, PerformanceStats performanceStats) {
        performanceStatsMap.put(testId, performanceStats);
    }

    public Map<String, PerformanceStats> getPerformanceStats() {
        return performanceStatsMap;
    }
}
