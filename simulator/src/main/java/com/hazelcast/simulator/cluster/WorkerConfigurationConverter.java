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
package com.hazelcast.simulator.cluster;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.worker.WorkerType;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;

import java.io.File;

import static com.hazelcast.simulator.coordinator.WorkerParameters.initClientHzConfig;
import static com.hazelcast.simulator.coordinator.WorkerParameters.initMemberHzConfig;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.worker.WorkerType.MEMBER;
import static java.lang.String.format;

public class WorkerConfigurationConverter implements Converter {

    private final int defaultHzPort;
    private final String licenseKey;

    private final WorkerParameters workerParameters;

    private final SimulatorProperties simulatorProperties;
    private final ComponentRegistry componentRegistry;

    public WorkerConfigurationConverter(int defaultHzPort, String licenseKey, WorkerParameters workerParameters,
                                        SimulatorProperties simulatorProperties, ComponentRegistry componentRegistry) {
        this.defaultHzPort = defaultHzPort;
        this.licenseKey = licenseKey;
        this.workerParameters = workerParameters;

        this.simulatorProperties = simulatorProperties;
        this.componentRegistry = componentRegistry;
    }

    @Override
    public boolean canConvert(Class type) {
        return (type == WorkerConfiguration.class);
    }

    @Override
    public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context) {
        WorkerConfiguration workerConfiguration = (WorkerConfiguration) source;

        writer.addAttribute("name", workerConfiguration.getName());
        writer.addAttribute("type", workerConfiguration.getType().name());
        writer.addAttribute("hzVersion", workerConfiguration.getHzVersion());
        writer.addAttribute("hzConfigFile", workerConfiguration.getHzConfig());
        writer.addAttribute("jvmOptions", workerConfiguration.getJvmOptions());
    }

    @Override
    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context) {
        String name = reader.getAttribute("name");
        String type = reader.getAttribute("type");
        String hzVersion = reader.getAttribute("hzVersion");
        String hzConfig = reader.getAttribute("hzConfig");
        String hzConfigFile = reader.getAttribute("hzConfigFile");
        String jvmOptions = reader.getAttribute("jvmOptions");

        WorkerType workerType = WorkerType.valueOf(type);
        if (hzVersion == null) {
            hzVersion = workerParameters.getHazelcastVersionSpec();
        }
        hzConfig = getHzConfig(hzConfig, hzConfigFile, workerType);
        if (jvmOptions == null) {
            jvmOptions = getDefaultJvmOptions(workerType);
        }

        return new WorkerConfiguration(name, workerType, hzVersion, hzConfig, jvmOptions);
    }

    private String getHzConfig(String hzConfig, String hzConfigFile, WorkerType workerType) {
        if (hzConfig != null) {
            return initHzConfig(hzConfig, workerType);
        }
        if (hzConfigFile != null) {
            return getHzConfigFromFile(hzConfigFile, workerType);
        }
        return getDefaultHzConfig(workerType);
    }

    private String initHzConfig(String hzConfig, WorkerType workerType) {
        if (workerType == MEMBER) {
            return initMemberHzConfig(hzConfig, componentRegistry, defaultHzPort, licenseKey, simulatorProperties);
        }
        return initClientHzConfig(hzConfig, componentRegistry, defaultHzPort, licenseKey);
    }

    private String getHzConfigFromFile(String hzConfigFile, WorkerType workerType) {
        File file = newFile(hzConfigFile);
        if (!file.exists()) {
            throw new IllegalArgumentException(format("Hazelcast configuration for Worker [%s] does not exist", file));
        }
        return initHzConfig(fileAsText(file), workerType);
    }

    private String getDefaultHzConfig(WorkerType workerType) {
        return (workerType == MEMBER) ? workerParameters.getMemberHzConfig() : workerParameters.getClientHzConfig();
    }

    private String getDefaultJvmOptions(WorkerType workerType) {
        return (workerType == MEMBER) ? workerParameters.getMemberJvmOptions() : workerParameters.getClientJvmOptions();
    }
}
