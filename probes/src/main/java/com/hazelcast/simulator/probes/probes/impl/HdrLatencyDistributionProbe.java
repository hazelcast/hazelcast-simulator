package com.hazelcast.simulator.probes.probes.impl;

import org.HdrHistogram.Histogram;

import java.util.concurrent.TimeUnit;

public class HdrLatencyDistributionProbe
        extends AbstractIntervalProbe<HdrLatencyDistributionResult, HdrLatencyDistributionProbe> {

    public static final long MAXIMUM_LATENCY = TimeUnit.SECONDS.toMicros(60);

    private final Histogram histogram = new Histogram(MAXIMUM_LATENCY, 4);

    @Override
    public void done() {
        if (started == 0) {
            throw new IllegalStateException("You have to call started() before done()");
        }
        histogram.recordValue((int) TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
        invocations++;
    }

    @Override
    public HdrLatencyDistributionResult getResult() {
        return new HdrLatencyDistributionResult(histogram);
    }
}
