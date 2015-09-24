package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.worker.performance.PerformanceState;

import java.util.HashMap;
import java.util.Map;

public class PerformanceStateOperation implements SimulatorOperation {

    private final Map<String, PerformanceState> performanceStates = new HashMap<String, PerformanceState>();

    private final SimulatorAddress workerAddress;

    public PerformanceStateOperation(SimulatorAddress workerAddress) {
        this.workerAddress = workerAddress;
    }

    public void addPerformanceState(String testId, PerformanceState performanceState) {
        performanceStates.put(testId, performanceState);
    }

    public SimulatorAddress getWorkerAddress() {
        return workerAddress;
    }

    public Map<String, PerformanceState> getPerformanceStates() {
        return performanceStates;
    }
}
