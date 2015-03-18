package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.probes.probes.LinearHistogram;

import java.util.concurrent.TimeUnit;

public class LatencyDistributionProbe implements IntervalProbe<LatencyDistributionResult, LatencyDistributionProbe> {

    private static final int MAXIMUM_LATENCY = (int) TimeUnit.SECONDS.toMicros(5);
    private static final int STEP = 10;

    private final LinearHistogram histogram = new LinearHistogram(MAXIMUM_LATENCY, STEP);

    private long started;

    @Override
    public void started() {
        started = System.nanoTime();
    }

    @Override
    public void done() {
        histogram.addValue((int) TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - started));
    }

    @Override
    public void startProbing(long time) {
    }

    @Override
    public void stopProbing(long time) {
    }

    @Override
    public LatencyDistributionResult getResult() {
        return new LatencyDistributionResult(histogram);
    }

    @Override
    public LatencyDistributionProbe createNew(Long arg) {
        return new LatencyDistributionProbe();
    }
}
