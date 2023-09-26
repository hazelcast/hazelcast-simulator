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

/**
 * A {@link Probe} implementation that doesn't do anything.
 *
 * This probe is used for code generation; when a user has defined a
 * {@link com.hazelcast.simulator.test.annotations.TimeStep} method with a probe argument,
 * but latency is not being tracked.
 */
public class EmptyProbe implements Probe {

    public static final EmptyProbe INSTANCE = new EmptyProbe();

    @Override
    public boolean isPartOfThroughput() {
        return false;
    }

    @Override
    public void done(long startNanos) {
    }

    @Override
    public void recordValue(long latencyNanos) {
    }

    @Override
    public void reset() {
    }
}
