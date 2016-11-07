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

import static java.lang.reflect.Modifier.isStatic;
import static java.util.concurrent.TimeUnit.MINUTES;

public class OptionsBuilder {

    private final Options options = new Options();

    public OptionsBuilder() {
        new SimulatorInstaller().install();
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
        this.options.memberConfig = memberConfig;
        return this;
    }

    public OptionsBuilder sessionId(String sessionId) {
        this.options.sessionId = sessionId;
        return this;
    }

    public OptionsBuilder test(Object testInstance) {
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
        options.testCase.setProperty("class", testClass.getName());
        return this;
    }

    public OptionsBuilder threadCount(int threadCount) {
        options.testCase.setProperty("threadCount", threadCount);
        return this;
    }

    public OptionsBuilder interval(String interval) {
        options.testCase.setProperty("interval", interval);
        return this;
    }

    public OptionsBuilder testProperty(String name, Object value) {
        options.testCase.setProperty(name, value);
        return this;
    }

    public OptionsBuilder version(String version) {
        this.options.versionSpec = "maven=" + version;
        return this;
    }

    public OptionsBuilder duration(long duration, TimeUnit timeUnit) {
        this.options.durationSeconds = timeUnit.toSeconds(duration);
        return this;
    }

    public OptionsBuilder durationMinutes(long durationMinutes) {
        return duration(durationMinutes, MINUTES);
    }

    public OptionsBuilder memberCount(int memberCount) {
        this.options.memberCount = memberCount;
        return this;
    }

    public OptionsBuilder clientCount(int clientCount) {
        this.options.clientCount = clientCount;
        return this;
    }

    public OptionsBuilder memberVmOptionsAppend(String jvmArgs) {
        this.options.memberVmOptions = this.options.memberVmOptions + jvmArgs + " ";
        return this;
    }

    public OptionsBuilder clientVmOptionsAppend(String jvmArgs) {
        this.options.clientVmOptions = this.options.clientVmOptions + jvmArgs + " ";
        return this;
    }

    public OptionsBuilder iterations(long iterations) {
        options.testCase.setProperty("iterations", iterations);
        return this;
    }

    public OptionsBuilder name(String name) {
        options.testCase.setProperty("name", name);
        return this;
    }

    public OptionsBuilder logFrequency(long logFrequency) {
        options.testCase.setProperty("logFrequency", logFrequency);
        return this;
    }

    public OptionsBuilder logRateMs(int logRateMs) {
        options.testCase.setProperty("logRateMs", logRateMs);
        return this;
    }

    public Options build() {
        // at the moment no validations are done; will be added in the future.
        return options;
    }
}
