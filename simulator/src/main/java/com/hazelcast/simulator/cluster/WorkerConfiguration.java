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

import com.hazelcast.simulator.worker.WorkerType;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;

class WorkerConfiguration {

    @XStreamAsAttribute
    private final String name;

    @XStreamAsAttribute
    private final String type;

    @XStreamAsAttribute
    private final String hzVersion;

    @XStreamAsAttribute
    private final String hzConfig;

    @XStreamAsAttribute
    private final String jvmOptions;

    public WorkerConfiguration(String name, WorkerType type, String hzVersion, String hzConfig, String jvmOptions) {
        this.name = name;
        this.type = type.name();
        this.hzVersion = hzVersion;
        this.hzConfig = hzConfig;
        this.jvmOptions = jvmOptions;
    }

    public String getName() {
        return name;
    }

    public WorkerType getType() {
        return WorkerType.valueOf(type);
    }

    public String getHzVersion() {
        return hzVersion;
    }

    public String getHzConfig() {
        return hzConfig;
    }

    public String getJvmOptions() {
        return jvmOptions;
    }
}
