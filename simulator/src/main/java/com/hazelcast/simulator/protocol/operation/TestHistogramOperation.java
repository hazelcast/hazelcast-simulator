package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;

import java.util.Map;

public class TestHistogramOperation implements SimulatorOperation {

    private final SimulatorAddress workerAddress;
    private final String testId;
    private final Map<String, String> probeHistograms;

    public TestHistogramOperation(SimulatorAddress workerAddress, String testId, Map<String, String> probeHistograms) {
        this.workerAddress = workerAddress;
        this.testId = testId;
        this.probeHistograms = probeHistograms;
    }

    public SimulatorAddress getWorkerAddress() {
        return workerAddress;
    }

    public String getTestId() {
        return testId;
    }

    public Map<String, String> getProbeHistograms() {
        return probeHistograms;
    }
}
