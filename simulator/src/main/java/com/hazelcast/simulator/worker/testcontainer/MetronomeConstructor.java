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
package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.worker.metronome.EmptyMetronome;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.metronome.SleepingMetronome;

import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.worker.testcontainer.PropertyBinding.toPropertyName;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MetronomeConstructor {

    private final Class<? extends Metronome> metronomeClass;
    private final Metronome masterMetronome;
    private final long intervalNanos;

    public MetronomeConstructor(String executionGroup, PropertyBinding binding, int threadCount) {
        String property = toPropertyName(executionGroup, "interval");
        String intervalString = binding.load(property);

        long intervalNanos = intervalString == null ? 0 : parseInterval(property, intervalString);

        double ratePerSecond = binding.loadAsDouble(
                toPropertyName(executionGroup, "ratePerSecond"), 0);

        if (ratePerSecond > 0) {
            intervalNanos = round(SECONDS.toNanos(1) / ratePerSecond);
        }

        // we read the metronome up front so we doing get an unused properties error if interval is 0,
        // but the user did configure a metronome.
        Class<SleepingMetronome> configuredMetronomeClass = binding.loadAsClass(
                toPropertyName(executionGroup, "metronomeClass"), SleepingMetronome.class);

        this.intervalNanos = intervalNanos;
        if (intervalNanos == 0) {
            this.metronomeClass = EmptyMetronome.class;
            this.masterMetronome = EmptyMetronome.INSTANCE;
        } else {
            this.metronomeClass = configuredMetronomeClass;

            Constructor<? extends Metronome> constructor;
            try {
                constructor = this.metronomeClass.getConstructor(Long.TYPE, Integer.TYPE, PropertyBinding.class, String.class);
            } catch (NoSuchMethodException e) {
                throw new IllegalTestException("Metronome [%s], does not have the right constructor", e);
            }

            try {
                masterMetronome = constructor.newInstance(intervalNanos, threadCount, binding, executionGroup);
            } catch (Exception e) {
                throw new IllegalTestException("Failed to create a master metronome instance", e);
            }
        }
    }

    public long getIntervalNanos() {
        return intervalNanos;
    }

    private static long parseInterval(String property, String value) {
        long duration;
        try {
            if (value.endsWith("ns")) {
                duration = parse(NANOSECONDS, 2, value);
            } else if (value.endsWith("us")) {
                duration = parse(MICROSECONDS, 2, value);
            } else if (value.endsWith("ms")) {
                duration = parse(MILLISECONDS, 2, value);
            } else if (value.endsWith("s")) {
                duration = parse(SECONDS, 1, value);
            } else if (value.endsWith("m")) {
                duration = parse(MINUTES, 1, value);
            } else if (value.endsWith("h")) {
                duration = parse(HOURS, 1, value);
            } else if (value.endsWith("d")) {
                duration = parse(DAYS, 1, value);
            } else {
                throw new IllegalTestException(format("%s is missing a timeunit in [%s]. For example 10us", property, value));
            }
        } catch (NumberFormatException e) {
            throw new IllegalTestException(format("%s has an invalid property value [%s]", property, value), e);
        }

        if (duration < 0) {
            throw new IllegalTestException(format("%s has with value [%s] must not be positive", property, value));
        }

        return duration;
    }

    private static long parse(TimeUnit unit, int skip, String value) {
        value = value.substring(0, value.length() - skip);
        long interval = Long.parseLong(value);
        return unit.toNanos(interval);
    }

    Class<? extends Metronome> getMetronomeClass() {
        return metronomeClass;
    }

    Metronome newInstance() {
        if (metronomeClass == EmptyMetronome.class) {
            return EmptyMetronome.INSTANCE;
        }

        try {
            Constructor<? extends Metronome> constructor = metronomeClass.getConstructor(Metronome.class);
            return constructor.newInstance(masterMetronome);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
