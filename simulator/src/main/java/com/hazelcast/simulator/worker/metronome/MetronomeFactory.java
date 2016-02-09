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
package com.hazelcast.simulator.worker.metronome;

import java.util.concurrent.TimeUnit;

import static java.lang.Math.round;

public final class MetronomeFactory {

    private static final Metronome EMPTY_METRONOME = new EmptyMetronome();

    private MetronomeFactory() {
    }

    /**
     * Creates a {@link Metronome} instance with a fixed millisecond interval of type {@link MetronomeType#SLEEPING}.
     *
     * @param intervalMs wait interval in milliseconds
     * @return a {@link Metronome} instance
     */
    public static Metronome withFixedIntervalMs(int intervalMs) {
        return withFixedIntervalMs(intervalMs, MetronomeType.SLEEPING);
    }

    /**
     * Creates a {@link Metronome} instance with a fixed frequency in Hz of type {@link MetronomeType#SLEEPING}.
     *
     * If the frequency is 0 Hz the method {@link Metronome#waitForNext()} will have no delay.
     *
     * @param frequency frequency in Hz
     * @return a {@link Metronome} instance
     */
    public static Metronome withFixedFrequency(float frequency) {
        return withFixedFrequency(frequency, MetronomeType.SLEEPING);
    }

    /**
     * Creates a {@link Metronome} instance with a fixed millisecond interval.
     *
     * @param intervalMs wait interval in milliseconds
     * @param type       {@link MetronomeType} to create
     * @return a {@link Metronome} instance
     */
    public static Metronome withFixedIntervalMs(int intervalMs, MetronomeType type) {
        if (intervalMs == 0) {
            return EMPTY_METRONOME;
        }
        switch (type) {
            case SLEEPING:
                return new SleepingMetronome(TimeUnit.MILLISECONDS.toNanos(intervalMs));
            default:
                return new BusySpinningMetronome(TimeUnit.MILLISECONDS.toNanos(intervalMs));
        }
    }

    /**
     * Creates a {@link Metronome} instance with a fixed frequency in Hz.
     *
     * If the frequency is 0 Hz the method {@link Metronome#waitForNext()} will have no delay.
     *
     * @param frequency frequency in Hz
     * @param type      {@link MetronomeType} to create
     * @return a {@link Metronome} instance
     */
    public static Metronome withFixedFrequency(float frequency, MetronomeType type) {
        if (frequency == 0) {
            return EMPTY_METRONOME;
        }
        long intervalNanos = round((double) TimeUnit.SECONDS.toNanos(1) / frequency);
        switch (type) {
            case SLEEPING:
                return new SleepingMetronome(intervalNanos);
            default:
                return new BusySpinningMetronome(intervalNanos);
        }
    }
}
