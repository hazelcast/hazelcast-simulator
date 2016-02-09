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
     * Defines if a probe should be considered to calculate the throughput of a test.
     *
     * @return <tt>true</tt> if probe is relevant for throughput, <tt>false</tt> otherwise
     */
    boolean isThroughputProbe();

    /**
     * Starts a latency measurement in the local thread.
     */
    void started();

    /**
     * Stops a latency measurement in the local thread and records the value.
     */
    void done();

    /**
     * Calculates the latency from an external start time and records the value.
     *
     * @param started external start time from {@link System#nanoTime()}.
     */
    void done(long started);

    /**
     * Adds a latency value in nanoseconds to the probe result.
     *
     * Can be used if {@link #started()} and {@link #done()} are not directly related, e.g. in asynchronous tests or are collected
     * from an external source like a C++ client.
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
     */
    Histogram getIntervalHistogram();
}
