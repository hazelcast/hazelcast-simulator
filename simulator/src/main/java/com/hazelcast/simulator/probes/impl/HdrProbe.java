/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.probes.impl;

import com.hazelcast.simulator.probes.Probe;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.concurrent.TimeUnit;

/**
 * Measures the latency distribution of a test.
 */
public class HdrProbe implements Probe {

    public static final long MAXIMUM_LATENCY = TimeUnit.SECONDS.toMicros(60);
    public static final int LATENCY_PRECISION = 4;

    private final Recorder recorder = new Recorder(MAXIMUM_LATENCY, LATENCY_PRECISION);

    private final boolean partOfTotalThroughput;

    public HdrProbe(boolean partOfTotalThroughput) {
        this.partOfTotalThroughput = partOfTotalThroughput;
    }

    @Override
    public boolean isMeasuringLatency() {
        return true;
    }

    @Override
    public boolean isPartOfTotalThroughput() {
        return partOfTotalThroughput;
    }

    @Override
    public void inc(long count) {
    }

    @Override
    public void done(long started) {
        long now = System.nanoTime();

        if (started <= 0) {
            throw new IllegalArgumentException("started has to be a positive number");
        }

        recordValue(now - started);
    }

    @Override
    public void recordValue(long latencyNanos) {
        int latencyMicros = (int) TimeUnit.NANOSECONDS.toMicros(latencyNanos);
        recorder.recordValue(latencyMicros > MAXIMUM_LATENCY ? MAXIMUM_LATENCY : (latencyMicros < 0 ? 0 : latencyMicros));
    }

    @Override
    public Histogram getIntervalHistogram() {
        return recorder.getIntervalHistogram();
    }

    @Override
    public long get() {
        throw new UnsupportedOperationException();
    }
}
