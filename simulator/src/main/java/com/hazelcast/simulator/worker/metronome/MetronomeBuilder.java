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
package com.hazelcast.simulator.worker.metronome;

import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static com.hazelcast.simulator.worker.metronome.MetronomeType.BUSY_SPINNING;
import static java.lang.Math.round;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MetronomeBuilder {

    public static final MetronomeType DEFAULT_METRONOME_TYPE = BUSY_SPINNING;

    private MetronomeType type = DEFAULT_METRONOME_TYPE;
    private boolean accountForCoordinatedOmission = true;
    private long intervalNanos;

    public MetronomeBuilder() {
    }

    public MetronomeBuilder withMetronomeType(MetronomeType type) {
        this.type = checkNotNull(type, "type cant be null");
        return this;
    }

    public MetronomeBuilder withRatePerSecond(double ratePerSecond) {
        long intervalMicros = round(SECONDS.toMicros(1) / ratePerSecond);
        return withIntervalMicros(intervalMicros);
    }

    public MetronomeBuilder withIntervalMicros(long intervalUs) {
        if (intervalUs < 0) {
            throw new IllegalArgumentException("intervalUs can't be smaller than 0, but was: " + intervalUs);
        }
        this.intervalNanos = MICROSECONDS.toNanos(intervalUs);
        return this;
    }

    public MetronomeBuilder withIntervalMillis(long intervalMillis) {
        if (intervalMillis < 0) {
            throw new IllegalArgumentException("intervalMillis can't be smaller than 0, but was: " + intervalMillis);
        }
        this.intervalNanos = MILLISECONDS.toNanos(intervalMillis);
        return this;
    }

    public MetronomeBuilder withAccountForCoordinatedOmission(boolean accountForCoordinatedOmission) {
        this.accountForCoordinatedOmission = accountForCoordinatedOmission;
        return this;
    }

    public Metronome build() {
        if (intervalNanos == 0 || type == MetronomeType.NOP) {
            return EmptyMetronome.INSTANCE;
        }

        switch (type) {
            case BUSY_SPINNING:
                return new BusySpinningMetronome(intervalNanos, accountForCoordinatedOmission);
            case SLEEPING:
                return new SleepingMetronome(intervalNanos, accountForCoordinatedOmission);
            default:
                throw new IllegalStateException("Unrecognized metronomeType: " + type);
        }
    }

    public Class<? extends Metronome> getMetronomeClass() {
        return build().getClass();
    }

    long getIntervalNanos() {
        return intervalNanos;
    }
}
