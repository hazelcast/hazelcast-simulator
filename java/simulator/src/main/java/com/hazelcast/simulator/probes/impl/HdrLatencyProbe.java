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

import com.hazelcast.simulator.probes.LatencyProbe;
import org.HdrHistogram.Recorder;

import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * HDR-Histogram implementation of the {@link LatencyProbe}.
 */
public class HdrLatencyProbe implements LatencyProbe {
    // we want to track up to 24-hour.
    static final long HIGHEST_TRACKABLE_VALUE_NANOS = DAYS.toNanos(1);

    private final AtomicLong negativeCount = new AtomicLong();

    // we care only about microsecond accuracy.
    private static final long LOWEST_DISCERNIBLE_VALUE = MICROSECONDS.toNanos(1);

    // since we care about u    s, the value should be 1000 according to the javadoc of Recorder.
    private static final int NUMBER_OF_SIGNIFICANT_VALUE_DIGITS = 3;

    // these settings come the website; just above the following link
    //https://github.com/HdrHistogram/HdrHistogram#histogram-variants-and-internal-representation
    private final Recorder recorder = new Recorder(
            LOWEST_DISCERNIBLE_VALUE,
            HIGHEST_TRACKABLE_VALUE_NANOS,
            NUMBER_OF_SIGNIFICANT_VALUE_DIGITS);

    private final boolean includeInThroughput;
    private final String name;

    public HdrLatencyProbe(String name, boolean includeInThroughput) {
        this.name = name;
        this.includeInThroughput = includeInThroughput;
    }

    @Override
    public boolean includeInThroughput() {
        return includeInThroughput;
    }

    @Override
    public void done(long startNanos) {
        if (startNanos <= 0) {
            throw new IllegalArgumentException("startedNanos has to be a positive number");
        }

        long nowNanos = System.nanoTime();
        recordValue(nowNanos - startNanos);
    }

    @Override
    public void recordValue(long latencyNanos) {

        if (latencyNanos < 0) {
            negativeCount.incrementAndGet();

            // Negative values should normally not happen.
            // But it could happen when the clock jump or when there is an
            // overflow. So lets convert it to a postive value and record it.
            if (latencyNanos == Long.MIN_VALUE) {
                latencyNanos = HIGHEST_TRACKABLE_VALUE_NANOS;
            } else {
                latencyNanos = -latencyNanos;
            }
        }

        if (latencyNanos > HIGHEST_TRACKABLE_VALUE_NANOS) {
            latencyNanos = HIGHEST_TRACKABLE_VALUE_NANOS;
        }
        recorder.recordValue(latencyNanos);
    }

    public Recorder getRecorder() {
        return recorder;
    }

    @Override
    public void reset() {
        recorder.reset();
    }

    @Override
    public long negativeCount() {
        return negativeCount.get();
    }

    @Override
    public String name() {
        return name;
    }
}
