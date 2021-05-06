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
    public int windowSize;
    public long slideBy;
    public ProcessingGuarantee guarantee;
    public int snapshotIntervalMillis;
    public int warmupSeconds;
    public int measurementSeconds;
    public int latencyReportingThresholdMs;

    public BenchmarkProperties() {
        this.eventsPerSecond = 1_000;
        this.numDistinctKeys = 10_000;
        this.windowSize = 1_000;
        this.slideBy = 5_000;
        this.guarantee = ProcessingGuarantee.NONE;
        this.snapshotIntervalMillis = 0;
        this.warmupSeconds = 0;
        this.measurementSeconds = 60;
        this.latencyReportingThresholdMs = 10;
    }

    public BenchmarkProperties(
            int eventsPerSecond,
            int numDistinctKeys,
            int windowSize,
            long slideBy,
            int warmupSeconds,
            int measurementSeconds
    ) {
        this(
                eventsPerSecond,
                numDistinctKeys,
                windowSize,
                slideBy,
                ProcessingGuarantee.NONE,
                0,
                warmupSeconds,
                measurementSeconds,
                10
        );
    }

    public BenchmarkProperties(
            int eventsPerSecond,
            int numDistinctKeys,
            int windowSize,
            long slideBy,
            ProcessingGuarantee guarantee,
            int snapshotIntervalMillis,
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
}
