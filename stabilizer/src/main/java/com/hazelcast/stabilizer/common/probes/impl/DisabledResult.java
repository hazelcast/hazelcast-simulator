package com.hazelcast.stabilizer.common.probes.impl;

import com.hazelcast.stabilizer.common.probes.Result;

public class DisabledResult implements Result<DisabledResult> {
    @Override
    public DisabledResult combine(DisabledResult other) {
        return this;
    }

    @Override
    public String toHumanString() {
        return "Probe Disabled";
    }
}
