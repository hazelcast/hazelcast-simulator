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

import com.hazelcast.simulator.probes.probes.LinearHistogram;

import java.util.concurrent.TimeUnit;

public class LatencyDistributionProbe extends AbstractIntervalProbe<LatencyDistributionResult, LatencyDistributionProbe> {

    private static final int MAXIMUM_LATENCY = (int) TimeUnit.SECONDS.toMicros(5);
    private static final int STEP = 10;

    private final LinearHistogram histogram = new LinearHistogram(MAXIMUM_LATENCY, STEP);

    @Override
    public void done() {
        if (started == 0) {
            throw new IllegalStateException("You have to call started() before done()");
        }
        histogram.addValue((int) TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - started));
        invocations++;
    }

    @Override
    public LatencyDistributionResult getResult() {
        return new LatencyDistributionResult(histogram);
    }
}
