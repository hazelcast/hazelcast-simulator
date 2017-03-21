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
package com.hazelcast.simulator.provisioner;

import com.google.common.base.Predicate;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import org.apache.log4j.Logger;
import org.jclouds.compute.domain.NodeMetadata;

import java.util.Map;

import static java.lang.String.format;

class NodeMetadataPredicate implements Predicate<NodeMetadata> {

    private static final Logger LOGGER = Logger.getLogger(NodeMetadataPredicate.class);

    private final ComponentRegistry registry;
    private final Map<String, AgentData> terminateMap;

    NodeMetadataPredicate(ComponentRegistry registry, Map<String, AgentData> terminateMap) {
        this.registry = registry;
        this.terminateMap = terminateMap;
    }

    @Override
    public boolean apply(NodeMetadata nodeMetadata) {
        for (String publicAddress : nodeMetadata.getPublicAddresses()) {
            AgentData agent = terminateMap.remove(publicAddress);
            if (agent != null) {
                LOGGER.info(format("    Terminating instance %s", publicAddress));
                registry.removeAgent(agent);
                return true;
            }
        }
        return false;
    }
}
