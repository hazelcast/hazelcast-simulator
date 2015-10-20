package com.hazelcast.simulator.protocol.operation;

import java.util.Map;

public class TestHistogramOperation implements SimulatorOperation {

    private final String testId;
    private final Map<String, String> probeHistograms;

    public TestHistogramOperation(String testId, Map<String, String> probeHistograms) {
        this.testId = testId;
        this.probeHistograms = probeHistograms;
    }

    public String getTestId() {
        return testId;
    }

    public Map<String, String> getProbeHistograms() {
        return probeHistograms;
    }
}
