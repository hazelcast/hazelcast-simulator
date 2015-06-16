/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.probes.probes.impl;

import org.HdrHistogram.Histogram;

import java.util.concurrent.TimeUnit;

public class HdrLatencyDistributionProbe
        extends AbstractIntervalProbe<HdrLatencyDistributionResult, HdrLatencyDistributionProbe> {

    public static final long MAXIMUM_LATENCY = TimeUnit.SECONDS.toMicros(60);

    private final Histogram histogram = new Histogram(MAXIMUM_LATENCY, 4);

    @Override
    public void recordValue(long latencyNanos) {
        histogram.recordValue((int) TimeUnit.NANOSECONDS.toMicros(latencyNanos));
    }

    @Override
    public long getInvocationCount() {
        return histogram.getTotalCount();
    }

    @Override
    public HdrLatencyDistributionResult getResult() {
        return new HdrLatencyDistributionResult(histogram);
    }
}
