package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.LinearHistogram;

import java.util.concurrent.TimeUnit;

public class LatencyDistributionProbe extends AbstractIntervalProbe<LatencyDistributionResult, LatencyDistributionProbe> {

    private static final int MAXIMUM_LATENCY = (int) TimeUnit.SECONDS.toMicros(5);
    private static final int STEP = 10;

    private final LinearHistogram histogram = new LinearHistogram(MAXIMUM_LATENCY, STEP);

    @Override
    public void done() {
        if (started == 0) {
            throw new IllegalStateException("You have to call started() before done()");
        }
        histogram.addValue((int) TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - started));
        invocations++;
    }

    @Override
    public LatencyDistributionResult getResult() {
        return new LatencyDistributionResult(histogram);
    }
}
