package com.hazelcast.stabilizer.probes.probes.impl;

import com.hazelcast.stabilizer.probes.probes.IntervalProbe;
import org.HdrHistogram.Histogram;

public class HdrLatencyDistributionProbe implements IntervalProbe<HdrLatencyProbeResult, HdrLatencyDistributionProbe> {
    public static final long MAXIMUM_LATENCY = 60 * 1000 * 1000; // 1 minute
    private final Histogram histogram = new Histogram(MAXIMUM_LATENCY, 4);

    private long started;


    @Override
    public void started() {
        started = System.nanoTime();
    }

    @Override
    public void done() {
        histogram.recordValue((System.nanoTime() - started) / 1000);
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
