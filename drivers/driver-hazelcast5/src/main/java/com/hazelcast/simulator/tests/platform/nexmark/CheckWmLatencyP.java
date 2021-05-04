package com.hazelcast.simulator.tests.platform.nexmark;

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;

import javax.annotation.Nonnull;

public class CheckWmLatencyP extends AbstractProcessor {

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        return tryEmit(ordinal, item);
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        System.out.format("WM is %,d ms behind real time%n", System.currentTimeMillis() - watermark.timestamp());
        return super.tryProcessWatermark(watermark);
    }
}
