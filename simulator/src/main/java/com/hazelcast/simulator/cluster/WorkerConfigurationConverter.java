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

import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.worker.WorkerType;
import com.thoughtworks.xstream.converters.Converter;
import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;

import static com.hazelcast.simulator.worker.WorkerType.MEMBER;

public class WorkerConfigurationConverter implements Converter {

    private final WorkerParameters workerParameters;

    public WorkerConfigurationConverter(WorkerParameters workerParameters) {
        this.workerParameters = workerParameters;
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
        if (hzConfig == null && hzConfigFile != null) {
            // TODO load hz configuration from file
            hzConfig = "content of file " + hzConfigFile;
        }
        if (hzConfig == null) {
            hzConfig = (workerType == MEMBER) ? workerParameters.getMemberHzConfig() : workerParameters.getMemberHzConfig();
        }
        if (jvmOptions == null) {
            jvmOptions = (workerType == MEMBER) ? workerParameters.getMemberJvmOptions() : workerParameters.getClientJvmOptions();
        }

        return new WorkerConfiguration(name, workerType, hzVersion, hzConfig, jvmOptions);
    }
}
