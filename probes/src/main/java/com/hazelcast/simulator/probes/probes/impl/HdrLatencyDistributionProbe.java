package com.hazelcast.simulator.probes.probes.impl;

import com.hazelcast.simulator.probes.probes.IntervalProbe;
import org.HdrHistogram.Histogram;

import java.util.concurrent.TimeUnit;

public class HdrLatencyDistributionProbe implements IntervalProbe<HdrLatencyProbeResult, HdrLatencyDistributionProbe> {

    public static final long MAXIMUM_LATENCY = TimeUnit.SECONDS.toMicros(60);

    private final Histogram histogram = new Histogram(MAXIMUM_LATENCY, 4);

    private long started;

    @Override
    public void started() {
        started = System.nanoTime();
    }

    @Override
    public void done() {
        histogram.recordValue((int) TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
    }

    @Override
    public void startProbing(long time) {
    }

    @Override
    public void stopProbing(long time) {
    }

    @Override
    public HdrLatencyProbeResult getResult() {
        return new HdrLatencyProbeResult(histogram);
    }

    @Override
    public HdrLatencyDistributionProbe createNew(Long arg) {
        return new HdrLatencyDistributionProbe();
    }
}
