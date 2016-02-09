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
package com.hazelcast.simulator.cluster;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@XStreamAlias("clusterConfiguration")
class ClusterConfiguration {

    @XStreamImplicit(itemFieldName = "workerConfiguration", keyFieldName = "name")
    private final Map<String, WorkerConfiguration> workerConfigurations = new HashMap<String, WorkerConfiguration>();

    @XStreamImplicit(itemFieldName = "nodeConfiguration")
    private final List<NodeConfiguration> nodeConfigurations = new ArrayList<NodeConfiguration>();

    public int size() {
        return nodeConfigurations.size();
    }

    public List<NodeConfiguration> getNodeConfigurations() {
        return nodeConfigurations;
    }

    public WorkerConfiguration getWorkerConfiguration(String name) {
        return workerConfigurations.get(name);
    }

    // just for testing
    void addNodeConfiguration(NodeConfiguration nodeConfiguration) {
        nodeConfigurations.add(nodeConfiguration);
    }

    // just for testing
    void addWorkerConfiguration(WorkerConfiguration workerConfiguration) {
        workerConfigurations.put(workerConfiguration.getName(), workerConfiguration);
    }
}
