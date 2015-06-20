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
package com.hazelcast.simulator.probes.probes;

public interface IntervalProbe<R extends Result<R>, T extends SimpleProbe<R, T>> extends SimpleProbe<R, T> {

    void started();

    /**
     * Adds a latency value in nanoseconds to the probe result.
     *
     * Can be used if {@link #started()} and {@link #done()} are not directly related, e.g. in asynchronous tests or are collected
     * from an external source like a C++ client.
     *
     * @param latencyNanos latency value in nanoseconds
     */
    void recordValue(long latencyNanos);
}
