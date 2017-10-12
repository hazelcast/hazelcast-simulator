/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.special;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.test.annotations.Run;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;

/**
 * Used to verify the latency measurement with a superficial constant latency value.
 */
public class PerformanceMonitorLatencyTest extends AbstractTest {

    private static final long LATENCY_NANOS = TimeUnit.MICROSECONDS.toNanos(20);

    @InjectProbe(useForThroughput = true)
    Probe probe;

    @Run
    public void run() {
        while (!testContext.isStopped()) {
            probe.recordValue(LATENCY_NANOS);
            sleepMillis(1);
        }
    }
}
