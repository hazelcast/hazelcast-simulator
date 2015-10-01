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

import com.hazelcast.simulator.probes.probes.Probe;
import org.HdrHistogram.Histogram;

import java.util.concurrent.TimeUnit;

/**
 * Measures the latency distribution of a test.
 */
public class ProbeImpl implements Probe {

    public static final long MAXIMUM_LATENCY = TimeUnit.SECONDS.toMicros(60);

    private static final double ONE_SECOND_IN_MS = TimeUnit.SECONDS.toMillis(1);

    private final Histogram histogram = new Histogram(MAXIMUM_LATENCY, 4);

    private long invocations;

    private long durationMs;
    private long startedProbing;
    private long started;

    private boolean disabled;

    @Override
    public void startProbing(long timeStamp) {
        startedProbing = timeStamp;
    }

    @Override
    public void stopProbing(long timeStamp) {
        if (timeStamp < 0) {
            throw new IllegalArgumentException("timeStamp must be zero or positive.");
        }
        if (startedProbing == 0) {
            throw new IllegalStateException("Can't get result as probe has not been started yet.");
        }

        long stopOrNow = (timeStamp == 0 ? System.currentTimeMillis() : timeStamp);
        durationMs = stopOrNow - startedProbing;
        if (durationMs < 0) {
            throw new IllegalArgumentException("durationMs must be positive, but was " + durationMs);
        }
    }

    @Override
    public void started() {
        started = System.nanoTime();
    }

    @Override
    public void done() {
        if (started == 0) {
            throw new IllegalStateException("You have to call started() before done()");
        }
        recordValue(System.nanoTime() - started);
    }

    @Override
    public void disable() {
        disabled = true;
    }

    @Override
    public boolean isDisabled() {
        return disabled;
    }

    @Override
    public void setValues(long durationMs, long invocations) {
        if (durationMs < 1) {
            throw new IllegalArgumentException("durationMs must be positive, but was " + durationMs);
        }
        if (invocations < 1) {
            throw new IllegalArgumentException("invocations must be positive, but was " + invocations);
        }

        this.durationMs = durationMs;
        this.invocations = invocations;
    }

    @Override
    public void recordValue(long latencyNanos) {
        histogram.recordValue((int) TimeUnit.NANOSECONDS.toMicros(latencyNanos));
        invocations++;
    }

    @Override
    public long getInvocationCount() {
        return invocations;
    }

    @Override
    public ResultImpl getResult() {
        return new ResultImpl(histogram, invocations, ((invocations * ONE_SECOND_IN_MS) / durationMs));
    }
}
