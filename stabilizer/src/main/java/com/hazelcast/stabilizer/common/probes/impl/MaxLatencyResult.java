package com.hazelcast.stabilizer.common.probes.impl;

import com.hazelcast.stabilizer.common.probes.Result;

public class MaxLatencyResult implements Result<MaxLatencyResult> {
    private final long maxLatency;

    public MaxLatencyResult(long maxLatency) {
        this.maxLatency = maxLatency;
    }

    @Override
    public MaxLatencyResult combine(MaxLatencyResult other) {
        if (other == null) {
            return this;
        }
        return new MaxLatencyResult(Math.max(maxLatency, other.maxLatency));
    }

    @Override
    public String toHumanString() {
        return "Maximum latency: "+maxLatency+" ms.";
    }
}
