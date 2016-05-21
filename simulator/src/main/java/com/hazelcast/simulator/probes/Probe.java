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
package com.hazelcast.simulator.probes;

import org.HdrHistogram.Histogram;

public interface Probe {

    /**
     * Checks if this probe measures latency.
     *
     * @return {@code true} if this probe measures latency, {@code false} otherwise.
     */
    boolean isMeasuringLatency();

    /**
     * Checks if a probe should be considered to calculate the throughput of a test.
     *
     * @return {@code true} if probe is relevant for throughput, {@code false} otherwise
     */
    boolean isPartOfTotalThroughput();

    /**
     * Calculates the latency from an external start time and records the value.
     *
     * @param started external start time from {@link System#nanoTime()}.
     */
    void done(long started);

    void inc(long count);

    /**
     * Adds a latency value in nanoseconds to the probe result.
     *
     * @param latencyNanos latency value in nanoseconds
     */
    void recordValue(long latencyNanos);

    /**
     * Get an interval {@link Histogram}, which will include a stable, consistent view of all latency values accumulated since the
     * last interval histogram was taken.
     *
     * Resets the latency values and starts accumulating value counts for the next interval.
     *
     * @return a {@link Histogram} containing the latency values accumulated since the last interval histogram was taken
     * @throws UnsupportedOperationException on lightweight implementations
     */
    Histogram getIntervalHistogram();

    /**
     * Get the number of iterations.
     *
     * @return the number of iterations.
     * @throws UnsupportedOperationException on non-lightweight implementations
     */
    long get();
}
