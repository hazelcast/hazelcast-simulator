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

package com.hazelcast.simulator.tests.platform.nexmark;

import com.hazelcast.jet.config.ProcessingGuarantee;

public class BenchmarkProperties {
    public int eventsPerSecond;
    public int numDistinctKeys;
    public long windowSize;
    public long slideBy;
    public ProcessingGuarantee guarantee;
    public long snapshotIntervalMillis;
    public int warmupSeconds;
    public int measurementSeconds;
    public int latencyReportingThresholdMs;

    public BenchmarkProperties(
            int eventsPerSecond,
            int numDistinctKeys,
            ProcessingGuarantee guarantee,
            long snapshotIntervalMillis,
            int warmupSeconds,
            int measurementSeconds,
            int latencyReportingThresholdMs
    ) {
        this(
                eventsPerSecond,
                numDistinctKeys,
                -1,
                -1,
                guarantee,
                snapshotIntervalMillis,
                warmupSeconds,
                measurementSeconds,
                latencyReportingThresholdMs
        );
    }

    public BenchmarkProperties(
            int eventsPerSecond,
            int numDistinctKeys,
            long windowSize,
            long slideBy,
            ProcessingGuarantee guarantee,
            long snapshotIntervalMillis,
            int warmupSeconds,
            int measurementSeconds,
            int latencyReportingThresholdMs
    ) {
        this.eventsPerSecond = eventsPerSecond;
        this.numDistinctKeys = numDistinctKeys;
        this.windowSize = windowSize;
        this.slideBy = slideBy;
        this.guarantee = guarantee;
        this.snapshotIntervalMillis = snapshotIntervalMillis;
        this.warmupSeconds = warmupSeconds;
        this.measurementSeconds = measurementSeconds;
        this.latencyReportingThresholdMs = latencyReportingThresholdMs;
    }

    @Override
    public String toString() {
        return String.format(
                "Events per second            %,d%n"
                        + "Distinct keys                %,d%n"
                        + "Window size                  %,d ms%n"
                        + "Sliding step                 %,d ms%n"
                        + "Processing guarantee         %s%n"
                        + "Snapshot interval            %,d ms%n"
                        + "Warmup period                %,d s%n"
                        + "Measurement period           %,d s%n"
                        + "Latency reporting threshold  %,d ms%n",
                eventsPerSecond,
                numDistinctKeys,
                windowSize,
                slideBy,
                guarantee,
                snapshotIntervalMillis,
                warmupSeconds,
                measurementSeconds,
                latencyReportingThresholdMs
        );
    }
}
