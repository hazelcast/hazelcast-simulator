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

package com.hazelcast.simulator.boot;

import com.hazelcast.config.Config;
import com.hazelcast.simulator.common.SimulatorProperties;

import java.lang.reflect.Field;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static java.lang.reflect.Modifier.isStatic;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class OptionsBuilder {

    private final Options options;

    public OptionsBuilder() {
        new SimulatorInstaller().install();
        this.options = new Options();
    }

    public Set<String> getIgnoredClasspath() {
        return options.ignoredClasspath;
    }

    public SimulatorProperties getSimulatorProperties() {
        return options.simulatorProperties;
    }

    public OptionsBuilder licenceKey(String licenceKey) {
        this.options.licenseKey = licenceKey;
        return this;
    }

    public OptionsBuilder memberConfig(Config memberConfig) {
        this.options.memberConfig = checkNotNull(memberConfig, "memberConfig can't be null");
        return this;
    }

    public OptionsBuilder sessionId(String sessionId) {
        this.options.sessionId = sessionId;
        return this;
    }

    public OptionsBuilder test(Object testInstance) {
        checkNotNull(testInstance, "testInstance can't be null");

        options.testCase.setId(testInstance.getClass().getSimpleName());
        options.testCase.setProperty("class", testInstance.getClass().getName());

        for (Field field : testInstance.getClass().getFields()) {
            if (isStatic(field.getModifiers())) {
                continue;
            }

            //only primitve/wrappers/class/enum

            try {
                Object v = field.get(testInstance);
                options.testCase.setProperty(field.getName(), v);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        return this;
    }

    public OptionsBuilder test(Class testClass) {
        checkNotNull(testClass, "testClass can't be null");

        options.testCase.setId(testClass.getSimpleName());
        options.testCase.setProperty("class", testClass.getName());
        return this;
    }

    public OptionsBuilder threadCount(int threadCount) {
        if (threadCount <= 0) {
            throw new IllegalArgumentException("threadCount must be larger than 0");
        }

        options.testCase.setProperty("threadCount", threadCount);
        return this;
    }

    public OptionsBuilder interval(long interval, TimeUnit unit) {
        if (interval < 0) {
            throw new IllegalArgumentException("interval can't be smaller than 0");
        }

        options.testCase.setProperty("interval", unit.toMicros(interval) + "us");
        return this;
    }

    public OptionsBuilder testProperty(String name, Object value) {
        options.testCase.setProperty(name, value);
        return this;
    }

    public OptionsBuilder performanceMonitorIntervalSeconds(long seconds) {
        options.simulatorProperties.set("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS", "" + seconds);
        return this;
    }

    public OptionsBuilder duration(long duration, TimeUnit timeUnit) {
        if (duration < 0) {
            throw new IllegalArgumentException("duration can't be smaller than 0");
        }

        this.options.durationSeconds = timeUnit.toSeconds(duration);
        return this;
    }

    public OptionsBuilder durationMinutes(long durationMinutes) {
        return duration(durationMinutes, MINUTES);
    }

    public OptionsBuilder durationSeconds(long durationSeconds) {
        return duration(durationSeconds, SECONDS);
    }

    public OptionsBuilder memberCount(int memberCount) {
        if (memberCount < 0) {
            throw new IllegalArgumentException("memberCount can't be smaller than 0");
        }

        this.options.memberCount = memberCount;
        return this;
    }

    public OptionsBuilder clientCount(int clientCount) {
        if (clientCount < 0) {
            throw new IllegalArgumentException("clientCount can't be smaller than 0");
        }

        this.options.clientCount = clientCount;
        return this;
    }

    public OptionsBuilder memberArgsAppend(String jvmArgs) {
        checkNotNull(jvmArgs, "jvmArgs can't be null");

        this.options.memberArgs = this.options.memberArgs + jvmArgs + " ";
        return this;
    }

    public OptionsBuilder clientArgsAppend(String jvmArgs) {
        checkNotNull(jvmArgs, "jvmArgs can't be null");

        this.options.clientArgs = this.options.clientArgs + jvmArgs + " ";
        return this;
    }

    public OptionsBuilder iterations(long iterations) {
        if (iterations < 0) {
            throw new IllegalArgumentException("iterations can't be smaller than 0");
        }

        options.testCase.setProperty("iterations", iterations);
        return this;
    }

    public OptionsBuilder name(String name) {
        options.testCase.setProperty("name", name);
        return this;
    }

    public OptionsBuilder logFrequency(long logFrequency) {
        if (logFrequency < 0) {
            throw new IllegalArgumentException("logFrequency can't be smaller than 0");
        }

        options.testCase.setProperty("logFrequency", logFrequency);
        return this;
    }

    public OptionsBuilder logRateMs(int logRateMs) {
        if (logRateMs < 0) {
            throw new IllegalArgumentException("logRateMs can't be smaller than 0");
        }

        options.testCase.setProperty("logRateMs", logRateMs);
        return this;
    }

    public Options build() {
        if (options.testCase.getClassname() == null) {
            throw new IllegalStateException("No test has been defined");
        }

        // at the moment no validations are done; will be added in the future.
        return options;
    }
}
