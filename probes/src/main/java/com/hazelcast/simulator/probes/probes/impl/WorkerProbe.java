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

/**
 * This probe will measure the throughput but won't save its result.
 *
 * It can be used to provide the information for performance measuring during a test without polluting the worker
 * directory with a result file.
 */
public class WorkerProbe extends AbstractIntervalProbe<DisabledResult, WorkerProbe> {

    @Override
    public void recordValue(long latencyNanos) {
        invocations++;
    }

    @Override
    public void done() {
        invocations++;
    }

    @Override
    public DisabledResult getResult() {
        return new DisabledResult();
    }
}
