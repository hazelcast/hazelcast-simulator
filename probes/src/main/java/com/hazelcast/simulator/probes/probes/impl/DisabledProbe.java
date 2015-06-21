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

import com.hazelcast.simulator.probes.probes.IntervalProbe;

public final class DisabledProbe implements IntervalProbe<DisabledResult, DisabledProbe> {

    public static final DisabledProbe INSTANCE = new DisabledProbe();

    private static final DisabledResult RESULT = new DisabledResult();

    private DisabledProbe() {
    }

    @Override
    public void started() {
    }

    @Override
    public void recordValue(long latencyNanos) {
    }

    @Override
    public void done() {
    }

    @Override
    public long getInvocationCount() {
        return 0;
    }

    @Override
    public void startProbing(long timeStamp) {
    }

    @Override
    public void stopProbing(long timeStamp) {
    }

    @Override
    public void setValues(long durationMs, int invocations) {
    }

    @Override
    public DisabledResult getResult() {
        return RESULT;
    }

    @Override
    public void disable() {
    }

    @Override
    public boolean isDisabled() {
        return true;
    }
}
