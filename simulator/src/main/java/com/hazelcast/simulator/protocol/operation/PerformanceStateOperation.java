package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.worker.performance.PerformanceState;

import java.util.HashMap;
import java.util.Map;

public class PerformanceStateOperation implements SimulatorOperation {

    private final Map<String, PerformanceState> performanceStates = new HashMap<String, PerformanceState>();

    public void addPerformanceState(String testId, PerformanceState performanceState) {
        performanceStates.put(testId, performanceState);
    }

    public Map<String, PerformanceState> getPerformanceStates() {
        return performanceStates;
    }
}
