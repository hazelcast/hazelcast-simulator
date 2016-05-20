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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures the throughput only.
 */
public class ThroughputProbe implements Probe {

    private final AtomicLong counter = new AtomicLong();
    private final boolean partOfTotalThroughput;

    public ThroughputProbe(boolean partOfTotalThroughput) {
        this.partOfTotalThroughput = partOfTotalThroughput;
    }

    @Override
    public boolean isMeasuringLatency() {
        return false;
    }

    @Override
    public boolean isPartOfTotalThroughput() {
        return partOfTotalThroughput;
    }

    @Override
    public void done(long started) {
        counter.incrementAndGet();
    }

    @Override
    public void recordValue(long latencyNanos) {
        counter.incrementAndGet();
    }

    @Override
    public Histogram getIntervalHistogram() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long get() {
        return counter.get();
    }
}
