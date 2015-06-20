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

public interface SimpleProbe<R extends Result<R>, T extends SimpleProbe<R, T>> {

    void done();

    long getInvocationCount();

    void startProbing(long timeStamp);

    void stopProbing(long timeStamp);

    /**
     * Sets the throughput result by passing invocations and duration in milliseconds.
     *
     * Can be used if {@link #startProbing(long)} and {@link #stopProbing(long)} are not directly related, e.g. in asynchronous
     * tests or are collected from an external source like a C++ client.
     *
     * @param durationMs  duration of sampling in milliseconds
     * @param invocations number of invocation during sampling period
     */
    void setValues(long durationMs, int invocations);

    R getResult();
}
