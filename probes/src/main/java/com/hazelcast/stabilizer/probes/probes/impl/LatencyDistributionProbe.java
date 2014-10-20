package com.hazelcast.stabilizer.probes.probes.impl;

import com.hazelcast.stabilizer.probes.probes.IntervalProbe;
import com.hazelcast.stabilizer.probes.probes.LinearHistogram;

public class LatencyDistributionProbe implements IntervalProbe<LatencyDistributionResult, LatencyDistributionProbe> {
    private static final int MAXIMUM_LATENCY = 5000000;
    private static final int STEP = 10;

    private final LinearHistogram histogram = new LinearHistogram(MAXIMUM_LATENCY, STEP);

    private long started;


    @Override
    public void started() {
        started = System.nanoTime();
    }

    @Override
    public void done() {
        histogram.addValue((int) ((System.nanoTime() - started) / 1000));
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
