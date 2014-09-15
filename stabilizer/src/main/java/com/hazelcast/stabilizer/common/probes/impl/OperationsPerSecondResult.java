package com.hazelcast.stabilizer.common.probes.impl;

import com.hazelcast.stabilizer.common.probes.Result;

import java.text.NumberFormat;
import java.util.Locale;

public class OperationsPerSecondResult implements Result<OperationsPerSecondResult> {
    private final double operationsPerSecond;
    public OperationsPerSecondResult(double operationsPerSecond) {
        this.operationsPerSecond = operationsPerSecond;
    }

    @Override
    public OperationsPerSecondResult combine(OperationsPerSecondResult other) {
        if (other == null) {
            return this;
        }
        return new OperationsPerSecondResult(operationsPerSecond + other.operationsPerSecond);
    }

    @Override
    public String toHumanString() {
        NumberFormat floatFormat = NumberFormat.getInstance(Locale.US);
        return "Operations / second: " + floatFormat.format(operationsPerSecond);
    }
}
